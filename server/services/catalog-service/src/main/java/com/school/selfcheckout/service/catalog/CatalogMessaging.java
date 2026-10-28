package com.school.selfcheckout.service.catalog;

import com.school.selfcheckout.messaging.EventPublisher;
import com.school.selfcheckout.messaging.KafkaClients;
import com.school.selfcheckout.messaging.ResetAcknowledger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * catalog-service on Kafka: it consumes reset requests, and publishes catalog
 * snapshots and its reset acknowledgements.
 */
@Configuration
public class CatalogMessaging {

    @Bean(destroyMethod = "close")
    public Producer<String, String> catalogProducer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.producer(bootstrap, "catalog-service");
    }

    @Bean
    public EventPublisher eventPublisher(Producer<String, String> catalogProducer) {
        return new EventPublisher(catalogProducer);
    }

    @Bean
    public ResetAcknowledger resetAcknowledger(EventPublisher eventPublisher) {
        return new ResetAcknowledger(eventPublisher, "catalog-service");
    }

    @Bean(destroyMethod = "close")
    public Consumer<String, String> resetRequestKafkaConsumer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.consumer(bootstrap, "catalog-reset-requests", 10);
    }
}
