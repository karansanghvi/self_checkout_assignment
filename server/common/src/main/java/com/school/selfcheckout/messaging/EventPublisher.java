package com.school.selfcheckout.messaging;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

/**
 * Synchronous publishing of single events: returns only once the broker has
 * acknowledged the record.
 *
 * For the low-volume control-plane events -- reset requests, catalog
 * snapshots, acknowledgements -- where the sender's next step depends on the
 * event having been stored. The high-volume scan and completion events do not
 * come through here; checkout's outbox relay batches those.
 */
public class EventPublisher {

    private final Producer<String, String> producer;

    public EventPublisher(Producer<String, String> producer) {
        this.producer = producer;
    }

    /** @throws IllegalStateException if the broker did not acknowledge the event */
    public void publish(Topic topic, String key, long epoch, Object payload) {
        String value = EventCodec.encode(epoch, 0, Instant.now(), payload);
        try {
            producer.send(new ProducerRecord<>(topic.topicName(), 0, key, value)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted publishing to " + topic.topicName(), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Kafka did not acknowledge an event on " + topic.topicName() + ": " + e.getCause().getMessage(),
                    e.getCause());
        }
    }
}
