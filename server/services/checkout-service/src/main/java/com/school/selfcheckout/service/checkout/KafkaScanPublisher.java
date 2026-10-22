package com.school.selfcheckout.service.checkout;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Publishes scans to the {@code scan-events} topic.
 *
 * Everything goes to partition 0 of a one-partition topic. The popularity
 * window counts "the most recent 1000 scans", which only means something if
 * all scans share one order; Kafka orders records only within a partition.
 *
 * A record's value is {@code epoch:seq:sku}. The epoch lets analytics discard
 * events from a previous run that were still in flight when it was reset; the
 * seq lets it discard duplicates, which an at-least-once relay can produce
 * after a crash.
 */
public class KafkaScanPublisher implements ScanPublisher {

    private final Producer<String, String> producer;
    private final String topic;

    public KafkaScanPublisher(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void publish(long epoch, List<OutboxEvent> events) {
        List<Future<RecordMetadata>> acks = new ArrayList<>(events.size());
        for (OutboxEvent event : events) {
            acks.add(producer.send(new ProducerRecord<>(
                    topic, 0, null, epoch + ":" + event.seq() + ":" + event.sku())));
        }
        producer.flush();
        for (Future<RecordMetadata> ack : acks) {
            try {
                ack.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for Kafka acks", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Kafka did not acknowledge a scan event", e.getCause());
            }
        }
    }
}
