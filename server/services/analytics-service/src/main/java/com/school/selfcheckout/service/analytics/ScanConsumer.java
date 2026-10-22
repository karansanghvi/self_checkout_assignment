package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.service.analytics.pipeline.PopularityPipeline;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The pipeline's data source: consumes the {@code scan-events} topic and
 * submits each scan, in order, to the {@link PopularityPipeline}.
 *
 * In week 4 this was ScanFeed, polling checkout's {@code scan_events} table
 * through the shared database. checkout_db is out of reach now, so
 * checkout-service publishes the same events to Kafka and this consumes
 * them. The two services share a topic and a three-field message format,
 * nothing else.
 *
 * Records are {@code epoch:seq:sku}. Two filters make the feed exact:
 * <ul>
 *   <li><b>epoch</b> -- events from an earlier run, still in the topic or in
 *       flight when the reset happened, are skipped;</li>
 *   <li><b>seq</b> -- the outbox relay delivers at least once, so a seq at or
 *       below the last one consumed is a redelivery and is skipped.</li>
 * </ul>
 *
 * The topic has one partition, read by {@code assign}, not a consumer group:
 * there is exactly one analytics consumer and it keeps its own position. On
 * startup it seeks to the current epoch's start offset from analytics_db and
 * replays, so a restarted analytics-service rebuilds its window and loses
 * nothing.
 *
 * KafkaConsumer is not thread-safe, so a reset requested over HTTP is handed
 * to the consumer thread, which applies it between polls.
 */
@Component
public class ScanConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ScanConsumer.class);

    static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    static final long ERROR_BACKOFF_MILLIS = 1_000;
    static final long RESET_TIMEOUT_SECONDS = 15;

    private record PendingReset(long epoch, CompletableFuture<Void> done) {
    }

    private final Consumer<String, String> consumer;
    private final PopularityRepository repository;
    private final PopularityPipeline pipeline;
    private final TopicPartition partition;
    private final AtomicReference<PendingReset> pendingReset = new AtomicReference<>();

    // Touched only by the consumer thread (or a test driving pollOnce directly).
    private long epoch = -1;
    private long lastSeq = 0;

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong stale = new AtomicLong();
    private volatile boolean running;
    private Thread thread;

    public ScanConsumer(Consumer<String, String> scanEventConsumer,
                        PopularityRepository repository,
                        PopularityPipeline pipeline,
                        @Value("${app.kafka.scan-topic}") String topic) {
        this.consumer = scanEventConsumer;
        this.repository = repository;
        this.pipeline = pipeline;
        this.partition = new TopicPartition(topic, 0);
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        thread = new Thread(this, "scan-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    @PreDestroy
    public synchronized void stop() {
        running = false;
        if (thread != null) {
            consumer.wakeup();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
        // The consumer itself is closed by Spring, which destroys its bean after this one.
    }

    @Override
    public void run() {
        while (running) {
            try {
                pollOnce();
            } catch (WakeupException e) {
                // A reset or stop wants the thread's attention; the loop handles both.
            } catch (RuntimeException e) {
                // Kafka or analytics_db unreachable. The topic holds the backlog.
                log.warn("Scan consumer pass failed, retrying: {}", e.getMessage());
                try {
                    Thread.sleep(ERROR_BACKOFF_MILLIS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * Applies any pending reset, then consumes one poll's worth of records.
     *
     * @return the number of records received (before filtering)
     */
    int pollOnce() {
        if (epoch < 0) {
            AnalyticsState state = repository.loadState();
            consumer.assign(List.of(partition));
            consumer.seek(partition, state.startOffset());
            startEpoch(state.epoch());
            log.info("Scan consumer replaying epoch {} from offset {}", state.epoch(), state.startOffset());
        }

        PendingReset reset = pendingReset.getAndSet(null);
        if (reset != null) {
            applyReset(reset);
        }

        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<String, String> record : records) {
            handle(record);
        }
        return records.count();
    }

    private void handle(ConsumerRecord<String, String> record) {
        String[] parts = record.value() == null ? new String[0] : record.value().split(":", 3);
        long eventEpoch;
        long seq;
        try {
            eventEpoch = Long.parseLong(parts[0]);
            seq = Long.parseLong(parts[1]);
        } catch (RuntimeException e) {
            log.warn("Ignoring malformed scan event at offset {}: {}", record.offset(), record.value());
            return;
        }

        if (eventEpoch < epoch) {
            stale.incrementAndGet();
            return;
        }
        if (eventEpoch > epoch) {
            // A reset this service never heard about -- it was down when
            // admin-service sent it. The events themselves say a new run
            // started here, so adopt it rather than discard the whole run.
            log.warn("Scan event from epoch {} while at {}; adopting it from offset {}",
                    eventEpoch, epoch, record.offset());
            repository.resetTo(eventEpoch, record.offset());
            startEpoch(eventEpoch);
        }
        if (seq <= lastSeq) {
            duplicates.incrementAndGet();
            return;
        }

        pipeline.submit(parts[2]);
        lastSeq = seq;
        consumed.incrementAndGet();
    }

    /**
     * Moves to {@code reset.epoch}. Its events start after everything now in
     * the topic: admin-service resets this service before checkout-service,
     * so checkout cannot yet have published a single event under the new
     * epoch.
     */
    private void applyReset(PendingReset reset) {
        try {
            long end = consumer.endOffsets(List.of(partition)).getOrDefault(partition, 0L);
            repository.resetTo(reset.epoch(), end);
            consumer.seek(partition, end);
            startEpoch(reset.epoch());
            log.info("Reset to epoch {}; its events start at offset {}", reset.epoch(), end);
            reset.done().complete(null);
        } catch (RuntimeException e) {
            reset.done().completeExceptionally(e);
            throw e;
        }
    }

    private void startEpoch(long newEpoch) {
        pipeline.reset(newEpoch);
        epoch = newEpoch;
        lastSeq = 0;
    }

    /**
     * Called by the reset endpoint: asks the consumer thread to move to
     * {@code newEpoch} and waits until it has.
     */
    public void reset(long newEpoch) {
        PendingReset reset = new PendingReset(newEpoch, new CompletableFuture<>());
        pendingReset.set(reset);
        consumer.wakeup();
        try {
            reset.done().get(RESET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CheckoutException.dependencyFailed("Interrupted waiting for the scan consumer to reset");
        } catch (ExecutionException e) {
            throw CheckoutException.dependencyFailed("Scan consumer could not reset: " + e.getCause().getMessage());
        } catch (TimeoutException e) {
            throw CheckoutException.dependencyFailed(
                    "Scan consumer did not reset within " + RESET_TIMEOUT_SECONDS + "s (is Kafka up?)");
        }
    }

    long lastSeq() {
        return lastSeq;
    }

    long consumed() {
        return consumed.get();
    }

    long duplicates() {
        return duplicates.get();
    }

    long stale() {
        return stale.get();
    }
}
