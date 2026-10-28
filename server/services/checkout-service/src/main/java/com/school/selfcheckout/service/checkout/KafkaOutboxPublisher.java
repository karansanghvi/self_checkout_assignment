package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.messaging.EventCodec;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Publishes outbox rows to the topic each one names.
 *
 * Everything goes to partition 0. The popularity window counts "the most
 * recent 1000 scans", which only means something if all scans share one
 * order, and Kafka orders records only within a partition.
 *
 * Each row's JSON body is wrapped in the common event envelope here, at
 * publish time, because only now is the epoch known for certain: the relay
 * holds its lock across the whole batch, so no reset can slip in between
 * stamping and sending. The envelope carries the outbox seq too, which lets
 * consumers drop the duplicates an at-least-once relay can produce after a
 * crash. The event id is {@code epoch-seq}, unique across runs.
 */
public class KafkaOutboxPublisher implements OutboxPublisher {

    private final Producer<String, String> producer;

    public KafkaOutboxPublisher(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void publish(long epoch, List<OutboxEvent> events) {
        List<Future<RecordMetadata>> acks = new ArrayList<>(events.size());
        for (OutboxEvent event : events) {
            String value = EventCodec.encodeRaw(epoch + "-" + event.seq(), event.type(), epoch, event.seq(),
                    event.createdAt(), event.payload());
            acks.add(producer.send(new ProducerRecord<>(event.topic(), 0, event.key(), value)));
        }
        producer.flush();
        for (Future<RecordMetadata> ack : acks) {
            try {
                ack.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for Kafka acks", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Kafka did not acknowledge an outbox event", e.getCause());
            }
        }
    }
}
