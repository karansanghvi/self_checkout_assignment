package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.contract.Events.Envelope;
import com.school.selfcheckout.contract.Events.ScanRecorded;
import com.school.selfcheckout.messaging.EventCodec;
import com.school.selfcheckout.messaging.EventCodec.MalformedEventException;
import com.school.selfcheckout.messaging.KafkaPollLoop;
import com.school.selfcheckout.messaging.Topic;
import com.school.selfcheckout.service.analytics.pipeline.PopularityPipeline;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The pipeline's data source: consumes the {@code scan-events} topic and
 * submits each scan, in order, to the {@link PopularityPipeline}.
 *
 * Each record is a {@code ScanRecorded} event in the common envelope. Two of
 * the envelope's fields make the feed exact:
 * <ul>
 *   <li><b>epoch</b> -- events from an earlier run, still in the topic or in
 *       flight when the reset happened, are skipped;</li>
 *   <li><b>seq</b> -- checkout's outbox sequence number. The relay delivers
 *       at least once, so a seq at or below the last one consumed is a
 *       redelivery and is skipped.</li>
 * </ul>
 *
 * The topic has one partition, read by {@code assign}, not a consumer group:
 * there is exactly one analytics consumer and it keeps its own position. On
 * startup it seeks to the current epoch's start offset from analytics_db and
 * replays, so a restarted analytics-service rebuilds its window and loses
 * nothing.
 *
 * A reset arrives from the catalog feed's thread and is run on this one,
 * between polls (see {@link KafkaPollLoop#callOnLoop}).
 */
@Component
public class ScanConsumer extends KafkaPollLoop {

    private static final Logger log = LoggerFactory.getLogger(ScanConsumer.class);

    static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    static final Duration RESET_TIMEOUT = Duration.ofSeconds(15);

    private final PopularityRepository repository;
    private final PopularityPipeline pipeline;
    private final TopicPartition partition = new TopicPartition(Topic.SCAN_EVENTS.topicName(), 0);

    // Touched only by the consumer thread (or a test driving step directly).
    private long epoch = -1;
    private long lastSeq = 0;

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong stale = new AtomicLong();

    public ScanConsumer(Consumer<String, String> scanEventConsumer,
                        PopularityRepository repository,
                        PopularityPipeline pipeline) {
        super("scan-consumer", scanEventConsumer);
        this.repository = repository;
        this.pipeline = pipeline;
    }

    @EventListener(ApplicationReadyEvent.class)
    @Override
    public synchronized void start() {
        super.start();
    }

    @PreDestroy
    @Override
    public synchronized void stop() {
        super.stop();
    }

    @Override
    protected void init() {
        AnalyticsState state = repository.loadState();
        consumer.assign(List.of(partition));
        consumer.seek(partition, state.startOffset());
        startEpoch(state.epoch());
        log.info("Scan consumer replaying epoch {} from offset {}", state.epoch(), state.startOffset());
    }

    @Override
    protected void poll() {
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<String, String> record : records) {
            handle(record);
        }
    }

    private void handle(ConsumerRecord<String, String> record) {
        Envelope envelope;
        ScanRecorded scan;
        try {
            envelope = EventCodec.decode(record.value());
            scan = EventCodec.payload(envelope, ScanRecorded.class);
        } catch (MalformedEventException e) {
            log.warn("Ignoring malformed scan event at offset {}: {}", record.offset(), e.getMessage());
            return;
        }
        long eventEpoch = envelope.epoch();

        if (eventEpoch < epoch) {
            stale.incrementAndGet();
            return;
        }
        if (eventEpoch > epoch) {
            // A reset this service has not yet heard about: checkout reset
            // first and is already publishing under the new epoch. The events
            // themselves say a new run started here, so adopt it now rather
            // than drop the run's first scans. The catalog snapshot that
            // follows then finds this service already in that epoch.
            log.warn("Scan event from epoch {} while at {}; adopting it from offset {}",
                    eventEpoch, epoch, record.offset());
            repository.resetTo(eventEpoch, record.offset());
            startEpoch(eventEpoch);
        }
        if (envelope.seq() <= lastSeq) {
            duplicates.incrementAndGet();
            return;
        }

        pipeline.submit(scan.sku());
        lastSeq = envelope.seq();
        consumed.incrementAndGet();
    }

    /**
     * Moves to {@code newEpoch}: its events start at the current position.
     * The consumer does not skip ahead to the end of the topic, because
     * checkout may already have reset and published new-epoch scans; anything
     * from the old epoch still ahead is dropped as stale instead.
     *
     * A no-op if the scan stream already moved this service to the epoch.
     */
    private void applyReset(long newEpoch) {
        if (newEpoch <= epoch) {
            return;
        }
        long start = consumer.position(partition);
        repository.resetTo(newEpoch, start);
        startEpoch(newEpoch);
        log.info("Reset to epoch {}; its events start at offset {}", newEpoch, start);
    }

    private void startEpoch(long newEpoch) {
        pipeline.reset(newEpoch);
        epoch = newEpoch;
        lastSeq = 0;
    }

    /** Called from the catalog feed: moves to {@code newEpoch} on the consumer thread and waits until it has. */
    public void reset(long newEpoch) {
        callOnLoop(() -> applyReset(newEpoch), RESET_TIMEOUT);
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
