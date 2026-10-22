package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;

@Configuration
@EnableScheduling
public class CheckoutConfiguration {

    /** Prices and names for scans and receipts, replicated from catalog-service. */
    @Bean
    public CatalogReplica catalogReplica(@Value("${app.catalog-url}") String catalogUrl) {
        return new CatalogReplica(catalogUrl);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void warmUpCatalog(ApplicationReadyEvent event) {
        event.getApplicationContext().getBean(CatalogReplica.class).warmUpInBackground();
    }

    /**
     * Idempotent, fully-acknowledged producer: {@code acks=all} plus
     * idempotence means a retried send is neither lost nor duplicated by the
     * producer, and with at most 5 requests in flight it also keeps order.
     */
    @Bean(destroyMethod = "close")
    public Producer<String, String> scanProducer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.CLIENT_ID_CONFIG, "checkout-outbox-relay",
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5,
                ProducerConfig.LINGER_MS_CONFIG, 5,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000),
                new StringSerializer(), new StringSerializer());
    }

    @Bean
    public ScanPublisher scanPublisher(Producer<String, String> scanProducer,
                                       @Value("${app.kafka.scan-topic}") String topic) {
        return new KafkaScanPublisher(scanProducer, topic);
    }
}
