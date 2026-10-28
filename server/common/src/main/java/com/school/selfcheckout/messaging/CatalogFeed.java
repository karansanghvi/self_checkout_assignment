package com.school.selfcheckout.messaging;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Events.CatalogSnapshot;
import com.school.selfcheckout.contract.Events.Envelope;
import com.school.selfcheckout.messaging.EventCodec.MalformedEventException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Follows the compacted {@code catalog-snapshots} topic, for every service
 * that needs the catalog but does not own it.
 *
 * A snapshot does two jobs:
 * <ol>
 *   <li><b>Replication.</b> Its items are swapped into the service's
 *       {@link CatalogReplica} (if it keeps one).</li>
 *   <li><b>The reset signal.</b> A snapshot from a newer epoch than the
 *       service is in means a reset happened: catalog-service has reseeded
 *       and everyone else must now follow. The service resets itself through
 *       its {@link ResetParticipant} and acknowledges on
 *       {@code reset-completed}. Because the reset reaches the other
 *       services only <i>through</i> catalog's snapshot, none of them can
 *       ever reseed from a stale catalog -- the ordering week 5's admin
 *       enforced by calling services in turn is now built into the event
 *       chain.</li>
 * </ol>
 *
 * <b>Startup.</b> The topic is read from the beginning. Compaction keeps it
 * to (roughly) one record, but until the end offset seen at startup is
 * reached only the latest record is kept, then handled once. So a service
 * that was down through a reset catches up with a single reset, not one per
 * missed run.
 *
 * <b>Acknowledgements.</b> A reset performed is always acknowledged. Once
 * caught up, a snapshot for the epoch the service is already in is
 * acknowledged again too: acks are idempotent (admin keeps one per service),
 * and a service that reached the epoch some other way -- analytics can adopt
 * one from the scan stream -- must still answer.
 *
 * If handling a snapshot fails, the consumer is rewound to it, so it is
 * retried rather than lost.
 */
public class CatalogFeed extends KafkaPollLoop {

    private static final Logger log = LoggerFactory.getLogger(CatalogFeed.class);

    static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    /** A service's side of a reset. */
    public interface ResetParticipant {

        /** The epoch this service's data is in. */
        long currentEpoch();

        /** Wipe run state and start {@code epoch}, using this catalog. */
        void resetTo(long epoch, CatalogSnapshot snapshot);
    }

    private final CatalogReplica replica;
    private final ResetParticipant participant;
    private final ResetAcknowledger acks;
    private final TopicPartition partition = new TopicPartition(Topic.CATALOG_SNAPSHOTS.topicName(), 0);

    private long catchUpOffset;
    private boolean caughtUp;
    private ConsumerRecord<String, String> latestDuringCatchUp;

    /** @param replica may be null for a service that only needs the reset signal */
    public CatalogFeed(Consumer<String, String> consumer, CatalogReplica replica,
                       ResetParticipant participant, ResetAcknowledger acks) {
        super(acks.service() + "-catalog-feed", consumer);
        this.replica = replica;
        this.participant = participant;
        this.acks = acks;
    }

    @Override
    protected void init() {
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
        catchUpOffset = consumer.endOffsets(List.of(partition)).getOrDefault(partition, 0L);
        caughtUp = catchUpOffset == 0;
        log.info("Catalog feed reading snapshots up to offset {}", catchUpOffset);
    }

    @Override
    protected void poll() {
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<String, String> record : records) {
            if (caughtUp) {
                handleOrRewind(record, true);
            } else {
                latestDuringCatchUp = record;
            }
        }
        if (!caughtUp && consumer.position(partition) >= catchUpOffset) {
            caughtUp = true;
            if (latestDuringCatchUp != null) {
                handleOrRewind(latestDuringCatchUp, false);
                latestDuringCatchUp = null;
            }
            log.info("Catalog feed caught up; epoch {}", participant.currentEpoch());
        }
    }

    public boolean isCaughtUp() {
        return caughtUp;
    }

    private void handleOrRewind(ConsumerRecord<String, String> record, boolean live) {
        try {
            handle(record, live);
        } catch (MalformedEventException e) {
            log.warn("Ignoring malformed catalog snapshot at offset {}: {}", record.offset(), e.getMessage());
        } catch (RuntimeException e) {
            consumer.seek(partition, record.offset());
            if (!live) {
                caughtUp = false;
            }
            throw e;
        }
    }

    private void handle(ConsumerRecord<String, String> record, boolean live) {
        Envelope envelope = EventCodec.decode(record.value());
        CatalogSnapshot snapshot = EventCodec.payload(envelope, CatalogSnapshot.class);
        long epoch = envelope.epoch();
        long current = participant.currentEpoch();

        if (replica != null && (epoch >= current || !replica.isLoaded())) {
            replica.apply(snapshot.items());
        }

        if (epoch > current) {
            long start = System.nanoTime();
            try {
                participant.resetTo(epoch, snapshot);
            } catch (RuntimeException e) {
                log.error("Reset to epoch {} failed: {}", epoch, e.getMessage());
                acks.failed(epoch, e.getMessage(), elapsedMs(start));
                return;
            }
            long millis = elapsedMs(start);
            log.info("Reset to epoch {} in {} ms ({} items)", epoch, millis, snapshot.items().size());
            acks.ok(epoch, millis);
        } else if (epoch == current && live) {
            acks.ok(epoch, 0);
        } else if (epoch < current) {
            log.info("Ignoring catalog snapshot from epoch {}; already at {}", epoch, current);
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
