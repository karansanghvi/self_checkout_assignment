package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.messaging.EventPublisher;
import com.school.selfcheckout.messaging.KafkaClients;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Supplier;

/** admin-service on Kafka: it publishes reset requests and reads the acknowledgements. */
@Configuration
public class AdminMessaging {

    @Bean(destroyMethod = "close")
    public Producer<String, String> adminProducer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.producer(bootstrap, "admin-service");
    }

    @Bean
    public EventPublisher eventPublisher(Producer<String, String> adminProducer) {
        return new EventPublisher(adminProducer);
    }

    /**
     * A fresh consumer per reset. Resets are rare, and a consumer that lives
     * only for the duration of one keeps admin-service free of any
     * background thread or state between requests.
     */
    @Bean
    public Supplier<Consumer<String, String>> resetAckConsumers(
            @Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return () -> KafkaClients.consumer(bootstrap, "admin-reset-acks", 100);
    }
}
