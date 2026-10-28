package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.messaging.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

/**
 * Parks an unprocessable completion on {@code transaction-completed.DLT},
 * unchanged, with why and where it came from in headers -- so it can be
 * inspected or replayed by hand without holding up every record behind it.
 *
 * Synchronous: the consumer saves its position past the record only after
 * this returns, so a dead letter cannot be lost.
 */
public class DeadLetters {

    private final Producer<String, String> producer;

    public DeadLetters(Producer<String, String> producer) {
        this.producer = producer;
    }

    public void send(ConsumerRecord<String, String> record, String reason) {
        ProducerRecord<String, String> letter = new ProducerRecord<>(
                Topic.TRANSACTION_COMPLETED_DLT.topicName(), 0, record.key(), record.value());
        letter.headers().add("x-reason", reason.getBytes(StandardCharsets.UTF_8));
        letter.headers().add("x-source-offset", Long.toString(record.offset()).getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(letter).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted dead-lettering offset " + record.offset(), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Could not dead-letter offset " + record.offset(), e.getCause());
        }
    }
}
