package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.messaging.CatalogFeed;
import com.school.selfcheckout.messaging.EventPublisher;
import com.school.selfcheckout.messaging.KafkaClients;
import com.school.selfcheckout.messaging.ResetAcknowledger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * inventory-service on Kafka. It consumes {@code transaction-completed} and
 * {@code catalog-snapshots}, and produces to {@code reset-completed} and the
 * dead-letter topic. It makes no HTTP call to any other service.
 */
@Configuration
public class InventoryConfiguration {

    @Bean(destroyMethod = "close")
    public Producer<String, String> inventoryProducer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.producer(bootstrap, "inventory-service");
    }

    @Bean
    public ResetAcknowledger resetAcknowledger(Producer<String, String> inventoryProducer) {
        return new ResetAcknowledger(new EventPublisher(inventoryProducer), "inventory-service");
    }

    @Bean(destroyMethod = "close")
    public Consumer<String, String> completionKafkaConsumer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.consumer(bootstrap, "inventory-completions", 2_000);
    }

    @Bean(destroyMethod = "stop")
    public CompletionConsumer completionConsumer(Consumer<String, String> completionKafkaConsumer,
                                                 InventoryRepository repository,
                                                 StockCommitService stockCommits,
                                                 Producer<String, String> inventoryProducer) {
        return new CompletionConsumer(completionKafkaConsumer, repository, stockCommits,
                new DeadLetters(inventoryProducer));
    }

    @Bean(destroyMethod = "close")
    public Consumer<String, String> catalogSnapshotConsumer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.consumer(bootstrap, "inventory-catalog-feed", 10);
    }

    /** Inventory keeps no catalog replica: it needs the snapshot only to reseed its stock rows. */
    @Bean(destroyMethod = "stop")
    public CatalogFeed catalogFeed(Consumer<String, String> catalogSnapshotConsumer,
                                   CompletionConsumer completionConsumer,
                                   ResetAcknowledger resetAcknowledger) {
        return new CatalogFeed(catalogSnapshotConsumer, null, completionConsumer, resetAcknowledger);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startConsumers(ApplicationReadyEvent event) {
        ApplicationContext context = event.getApplicationContext();
        context.getBean(CompletionConsumer.class).start();
        context.getBean(CatalogFeed.class).start();
    }
}
