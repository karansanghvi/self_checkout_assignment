package com.school.selfcheckout.messaging;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * The one place Kafka clients are configured, so every service gets the same
 * delivery guarantees.
 */
public final class KafkaClients {

    /** A catalog snapshot is one record; leave room for catalogs far bigger than the default 2,000 items. */
    public static final int MAX_MESSAGE_BYTES = 10 * 1024 * 1024;

    private KafkaClients() {
    }

    /**
     * Idempotent, fully-acknowledged producer: {@code acks=all} plus
     * idempotence means a retried send is neither lost nor duplicated by the
     * producer, and with at most 5 requests in flight it also keeps order.
     */
    public static Producer<String, String> producer(String bootstrapServers, String clientId) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, clientId,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5,
                ProducerConfig.LINGER_MS_CONFIG, 5,
                ProducerConfig.MAX_REQUEST_SIZE_CONFIG, MAX_MESSAGE_BYTES,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000),
                new StringSerializer(), new StringSerializer());
    }

    /**
     * A consumer with no group id and no auto-commit. Every consumer in the
     * system assigns its partitions itself and keeps its position wherever
     * its correctness needs it -- usually in its own database, in the same
     * transaction as the effects of the records it read.
     */
    public static Consumer<String, String> consumer(String bootstrapServers, String clientId, int maxPollRecords) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.CLIENT_ID_CONFIG, clientId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords,
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, MAX_MESSAGE_BYTES,
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000),
                new StringDeserializer(), new StringDeserializer());
    }
}
