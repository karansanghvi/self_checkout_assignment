package com.school.selfcheckout.service.catalog;

import com.school.selfcheckout.contract.Events.Envelope;
import com.school.selfcheckout.contract.Events.ResetRequested;
import com.school.selfcheckout.messaging.EventCodec;
import com.school.selfcheckout.messaging.EventCodec.MalformedEventException;
import com.school.selfcheckout.messaging.KafkaPollLoop;
import com.school.selfcheckout.messaging.Topic;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Consumes {@code reset-requested}, which only admin-service publishes.
 *
 * Starts at the end of the topic: a reset is a request for <i>now</i>, and
 * replaying old ones after a restart would only redo work. If catalog was
 * down when a reset was requested, admin-service times out waiting for its
 * ack and reports it; re-running the reset is safe.
 */
@Component
public class ResetRequestConsumer extends KafkaPollLoop {

    private static final Logger log = LoggerFactory.getLogger(ResetRequestConsumer.class);

    private final CatalogResetService resetService;
    private final TopicPartition partition = new TopicPartition(Topic.RESET_REQUESTED.topicName(), 0);

    public ResetRequestConsumer(Consumer<String, String> resetRequestKafkaConsumer, CatalogResetService resetService) {
        super("reset-request-consumer", resetRequestKafkaConsumer);
        this.resetService = resetService;
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
        consumer.assign(List.of(partition));
        consumer.seekToEnd(List.of(partition));
        consumer.position(partition); // resolve the lazy seek now, not at the first poll
    }

    @Override
    protected void poll() {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
            Envelope envelope;
            ResetRequested request;
            try {
                envelope = EventCodec.decode(record.value());
                request = EventCodec.payload(envelope, ResetRequested.class);
            } catch (MalformedEventException e) {
                log.warn("Ignoring malformed reset request at offset {}: {}", record.offset(), e.getMessage());
                continue;
            }
            try {
                resetService.handle(envelope.epoch(), request);
            } catch (RuntimeException e) {
                consumer.seek(partition, record.offset()); // retry it, rather than lose the reset
                throw e;
            }
        }
    }
}
