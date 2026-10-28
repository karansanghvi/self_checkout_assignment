package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Events.CatalogSnapshot;
import com.school.selfcheckout.contract.Events.CompletedLine;
import com.school.selfcheckout.contract.Events.Envelope;
import com.school.selfcheckout.contract.Events.TransactionCompleted;
import com.school.selfcheckout.messaging.CatalogFeed;
import com.school.selfcheckout.messaging.EventCodec;
import com.school.selfcheckout.messaging.EventCodec.MalformedEventException;
import com.school.selfcheckout.messaging.KafkaPollLoop;
import com.school.selfcheckout.messaging.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes {@code transaction-completed} and applies each poll's records as
 * one batch, through {@link StockCommitService}.
 *
 * The topic has one partition, read by {@code assign}, not a consumer group.
 * The position lives in inventory_db, saved with each batch; on startup this
 * seeks to it, so a restarted inventory-service resumes exactly where its
 * committed state ends.
 *
 * Each record is sorted into one of four cases:
 * <ul>
 *   <li><b>older epoch</b> -- a completion from before the last reset, still
 *       in the topic or in flight. Dropped: its stock no longer exists.</li>
 *   <li><b>newer epoch</b> -- checkout has already reset to a run this
 *       service has not reset to yet (its catalog snapshot has not arrived
 *       here). It cannot be applied to the old stock, so the partition is
 *       rewound to it and paused until the reset arrives.</li>
 *   <li><b>unprocessable</b> -- malformed, or naming a SKU inventory has no
 *       row for. Retrying cannot help, so it is parked on
 *       {@code transaction-completed.DLT} rather than block the partition
 *       forever.</li>
 *   <li><b>applicable</b> -- added to the batch.</li>
 * </ul>
 *
 * It is also inventory's {@link CatalogFeed.ResetParticipant}. A reset
 * arrives on the catalog feed's thread and is handed to this one with
 * {@link #callOnLoop}, so the reseed, the epoch change and the resume all
 * happen between two polls, never in the middle of a batch.
 */
public class CompletionConsumer extends KafkaPollLoop implements CatalogFeed.ResetParticipant {

    private static final Logger log = LoggerFactory.getLogger(CompletionConsumer.class);

    static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    static final Duration RESET_TIMEOUT = Duration.ofSeconds(30);

    private final InventoryRepository repository;
    private final StockCommitService stockCommits;
    private final DeadLetters deadLetters;
    private final TopicPartition partition = new TopicPartition(Topic.TRANSACTION_COMPLETED.topicName(), 0);

    // Touched only by the loop thread.
    private long epoch;
    private Set<String> knownSkus = Set.of();
    private boolean paused;

    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong replayed = new AtomicLong();
    private final AtomicLong stale = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();

    public CompletionConsumer(Consumer<String, String> consumer, InventoryRepository repository,
                              StockCommitService stockCommits, DeadLetters deadLetters) {
        super("completion-consumer", consumer);
        this.repository = repository;
        this.stockCommits = stockCommits;
        this.deadLetters = deadLetters;
    }

    @Override
    protected void init() {
        InventoryState state = repository.loadState();
        epoch = state.epoch();
        knownSkus = repository.findAllSkus();
        consumer.assign(List.of(partition));
        consumer.seek(partition, state.nextOffset());
        log.info("Completion consumer at epoch {}, resuming from offset {}", epoch, state.nextOffset());
    }

    @Override
    protected void poll() {
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        if (records.isEmpty()) {
            return;
        }
        List<TransactionCompleted> batch = new ArrayList<>(records.count());
        long firstOffset = -1;
        long nextOffset = -1;
        for (ConsumerRecord<String, String> record : records) {
            if (firstOffset < 0) {
                firstOffset = record.offset();
            }
            Envelope envelope;
            try {
                envelope = EventCodec.decode(record.value());
            } catch (MalformedEventException e) {
                deadLetter(record, e.getMessage());
                nextOffset = record.offset() + 1;
                continue;
            }
            if (envelope.epoch() < epoch) {
                stale.incrementAndGet();
                nextOffset = record.offset() + 1;
                continue;
            }
            if (envelope.epoch() > epoch) {
                log.info("Completion from epoch {} while at {}; pausing until this service resets",
                        envelope.epoch(), epoch);
                consumer.seek(partition, record.offset());
                consumer.pause(List.of(partition));
                paused = true;
                break;
            }
            String problem;
            TransactionCompleted completion = null;
            try {
                completion = EventCodec.payload(envelope, TransactionCompleted.class);
                problem = validate(completion);
            } catch (MalformedEventException e) {
                problem = e.getMessage();
            }
            if (problem != null) {
                deadLetter(record, problem);
            } else {
                batch.add(completion);
            }
            nextOffset = record.offset() + 1;
        }
        if (nextOffset < 0) {
            return; // paused on the very first record; nothing consumed
        }
        try {
            StockCommitService.BatchResult result = stockCommits.applyBatch(batch, nextOffset);
            applied.addAndGet(result.applied());
            replayed.addAndGet(result.replayed());
        } catch (RuntimeException e) {
            // Nothing was committed: rewind, so the whole batch is retried.
            consumer.seek(partition, firstOffset);
            if (paused) {
                consumer.resume(List.of(partition));
                paused = false;
            }
            throw e;
        }
    }

    /** @return why the completion can never be applied, or null if it can */
    private String validate(TransactionCompleted completion) {
        try {
            UUID.fromString(completion.transactionId());
        } catch (IllegalArgumentException | NullPointerException e) {
            return "transactionId is not a UUID";
        }
        if (completion.lines() == null || completion.lines().isEmpty()) {
            return "no lines";
        }
        for (CompletedLine line : completion.lines()) {
            if (line == null || line.sku() == null || line.quantity() <= 0) {
                return "a line without a sku or with a non-positive quantity";
            }
            if (!knownSkus.contains(line.sku())) {
                return "unknown SKU " + line.sku();
            }
        }
        return null;
    }

    private void deadLetter(ConsumerRecord<String, String> record, String reason) {
        log.warn("Dead-lettering transaction-completed offset {}: {}", record.offset(), reason);
        deadLetters.send(record, reason);
        deadLettered.incrementAndGet();
    }

    // --- CatalogFeed.ResetParticipant --------------------------------------

    /** Read from inventory_db, the source of truth, since the feed thread asks. */
    @Override
    public long currentEpoch() {
        return repository.loadState().epoch();
    }

    @Override
    public void resetTo(long newEpoch, CatalogSnapshot snapshot) {
        callOnLoop(() -> {
            repository.reseed(newEpoch, snapshot.items(), snapshot.stockPerItem());
            epoch = newEpoch;
            Set<String> skus = new HashSet<>(snapshot.items().size() * 2);
            for (CatalogItem item : snapshot.items()) {
                skus.add(item.sku());
            }
            knownSkus = skus;
            if (paused) {
                consumer.resume(List.of(partition));
                paused = false;
            }
        }, RESET_TIMEOUT);
    }

    long applied() {
        return applied.get();
    }

    long replayed() {
        return replayed.get();
    }

    long stale() {
        return stale.get();
    }

    long deadLettered() {
        return deadLettered.get();
    }

    boolean isPaused() {
        return paused;
    }
}
