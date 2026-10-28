package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.contract.Events.Envelope;
import com.school.selfcheckout.contract.Events.ResetCompleted;
import com.school.selfcheckout.contract.Events.ResetRequested;
import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.messaging.EventCodec;
import com.school.selfcheckout.messaging.EventCodec.MalformedEventException;
import com.school.selfcheckout.messaging.EventPublisher;
import com.school.selfcheckout.messaging.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Resets the whole system for a new load-test run -- by publishing one event,
 * then waiting for everyone's acknowledgement.
 *
 * In week 5 admin-service called {@code POST /internal/reset} on each service
 * in a fixed order. Now it calls no one:
 *
 * <ol>
 *   <li>It publishes {@code ResetRequested} on {@code reset-requested}, under
 *       a new epoch.</li>
 *   <li>catalog-service reseeds and publishes a catalog snapshot for that
 *       epoch; inventory, checkout and analytics each reset when they see
 *       it. The order week 5 enforced here -- catalog first -- is now the
 *       shape of the event chain itself.</li>
 *   <li>Each of the four publishes {@code ResetCompleted} on
 *       {@code reset-completed}. This collects them, matched by epoch, and
 *       answers the HTTP request once all four are in.</li>
 * </ol>
 *
 * The ack consumer is positioned at the end of {@code reset-completed}
 * <i>before</i> the request is published, so no ack can arrive unseen.
 *
 * A failed ack fails the reset at once, naming the service; a missing one
 * fails it at the timeout, naming who did not answer. Either way re-running
 * is safe: every step is a wipe-and-reseed under a newer epoch.
 *
 * The epoch is the reset's wall-clock time in milliseconds. That needs no
 * counter -- admin-service is stateless -- and it increases from one reset to
 * the next, which is all the services compare.
 */
@Service
public class ResetService {

    private static final Logger log = LoggerFactory.getLogger(ResetService.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final EventPublisher publisher;
    private final Supplier<Consumer<String, String>> ackConsumers;
    private final List<String> participants;
    private final Duration timeout;
    private final int defaultCatalogSize;
    private final int defaultStockPerItem;
    private final TopicPartition ackPartition = new TopicPartition(Topic.RESET_COMPLETED.topicName(), 0);

    private long lastEpoch;

    @Autowired
    public ResetService(EventPublisher publisher,
                        Supplier<Consumer<String, String>> ackConsumers,
                        @Value("${app.reset.participants}") List<String> participants,
                        @Value("${app.reset.timeout-ms:30000}") long timeoutMs,
                        @Value("${app.catalog-size:2000}") int defaultCatalogSize,
                        @Value("${app.stock-per-item:10000}") int defaultStockPerItem) {
        this.publisher = publisher;
        this.ackConsumers = ackConsumers;
        this.participants = List.copyOf(participants);
        this.timeout = Duration.ofMillis(timeoutMs);
        this.defaultCatalogSize = defaultCatalogSize;
        this.defaultStockPerItem = defaultStockPerItem;
    }

    public Map<String, Object> reset() {
        return reset(null, null);
    }

    /**
     * Resets to the given sizes, falling back to configuration for either
     * argument that is null. One reset at a time: a second caller waits.
     */
    public synchronized Map<String, Object> reset(Integer catalogSizeOverride, Integer stockPerItemOverride) {
        int catalogSize = catalogSizeOverride != null ? catalogSizeOverride : defaultCatalogSize;
        int stockPerItem = stockPerItemOverride != null ? stockPerItemOverride : defaultStockPerItem;
        if (catalogSize <= 0) {
            throw CheckoutException.invalidRequest("catalogSize must be positive");
        }
        if (stockPerItem < 0) {
            throw CheckoutException.invalidRequest("stockPerItem must not be negative");
        }
        long epoch = Math.max(System.currentTimeMillis(), lastEpoch + 1);
        lastEpoch = epoch;

        long start = System.nanoTime();
        Map<String, ResetCompleted> acks;
        try (Consumer<String, String> consumer = ackConsumers.get()) {
            consumer.assign(List.of(ackPartition));
            consumer.seekToEnd(List.of(ackPartition));
            consumer.position(ackPartition); // resolve the seek before publishing, so no ack is missed

            try {
                publisher.publish(Topic.RESET_REQUESTED, null, epoch, new ResetRequested(catalogSize, stockPerItem));
            } catch (IllegalStateException e) {
                throw CheckoutException.dependencyFailed("Could not publish the reset request: " + e.getMessage());
            }
            acks = awaitAcks(consumer, epoch, start);
        }
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset complete: {} items x {} units in {} ms, epoch {}", catalogSize, stockPerItem, millis, epoch);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("catalogSize", catalogSize);
        result.put("stockPerItem", stockPerItem);
        result.put("epoch", epoch);
        result.put("elapsedMs", millis);
        result.put("services", participants.stream().map(acks::get).toList());
        return result;
    }

    private Map<String, ResetCompleted> awaitAcks(Consumer<String, String> consumer, long epoch, long startNanos) {
        Map<String, ResetCompleted> acks = new LinkedHashMap<>();
        long deadline = startNanos + timeout.toNanos();
        while (!acks.keySet().containsAll(participants)) {
            if (System.nanoTime() >= deadline) {
                List<String> missing = new ArrayList<>(participants);
                missing.removeAll(acks.keySet());
                log.error("Reset to epoch {} timed out; no ack from {}", epoch, missing);
                throw CheckoutException.dependencyFailed("Reset to epoch " + epoch + " timed out after "
                        + timeout.toSeconds() + "s waiting for " + missing + "; acknowledged: " + acks.keySet());
            }
            for (ConsumerRecord<String, String> record : consumer.poll(POLL_TIMEOUT)) {
                ResetCompleted ack;
                try {
                    Envelope envelope = EventCodec.decode(record.value());
                    if (envelope.epoch() != epoch) {
                        continue; // an ack from an earlier reset, or a re-ack
                    }
                    ack = EventCodec.payload(envelope, ResetCompleted.class);
                } catch (MalformedEventException e) {
                    log.warn("Ignoring malformed reset ack at offset {}: {}", record.offset(), e.getMessage());
                    continue;
                }
                if (!ack.ok()) {
                    log.error("Reset to epoch {} failed at {}: {}", epoch, ack.service(), ack.error());
                    throw CheckoutException.dependencyFailed("Reset failed at " + ack.service() + " after "
                            + acks.size() + " service(s) had reset: " + ack.error());
                }
                acks.putIfAbsent(ack.service(), ack);
            }
        }
        return acks;
    }
}
