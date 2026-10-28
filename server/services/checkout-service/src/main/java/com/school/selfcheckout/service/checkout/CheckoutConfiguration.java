package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Events.CatalogSnapshot;
import com.school.selfcheckout.messaging.CatalogFeed;
import com.school.selfcheckout.messaging.EventPublisher;
import com.school.selfcheckout.messaging.KafkaClients;
import com.school.selfcheckout.messaging.ResetAcknowledger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * checkout-service on Kafka. It produces to {@code scan-events} and
 * {@code transaction-completed} (through the outbox) and to
 * {@code reset-completed}, and consumes {@code catalog-snapshots}. It makes
 * no HTTP call to any other service.
 */
@Configuration
public class CheckoutConfiguration {

    /** Prices and names for scans and receipts, replicated from the catalog-snapshots topic. */
    @Bean
    public CatalogReplica catalogReplica() {
        return new CatalogReplica();
    }

    /** One producer for everything checkout sends; KafkaProducer is thread-safe. */
    @Bean(destroyMethod = "close")
    public Producer<String, String> checkoutProducer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.producer(bootstrap, "checkout-service");
    }

    @Bean
    public OutboxPublisher outboxPublisher(Producer<String, String> checkoutProducer) {
        return new KafkaOutboxPublisher(checkoutProducer);
    }

    @Bean
    public ResetAcknowledger resetAcknowledger(Producer<String, String> checkoutProducer) {
        return new ResetAcknowledger(new EventPublisher(checkoutProducer), "checkout-service");
    }

    @Bean(destroyMethod = "close")
    public Consumer<String, String> catalogSnapshotConsumer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.consumer(bootstrap, "checkout-catalog-feed", 10);
    }

    /**
     * On a snapshot from a newer epoch: empty checkout_db and move the outbox
     * relay to that epoch. The relay's lock means no batch from the old run
     * can be published under the new epoch.
     */
    @Bean(destroyMethod = "stop")
    public CatalogFeed catalogFeed(Consumer<String, String> catalogSnapshotConsumer, CatalogReplica catalogReplica,
                                   OutboxRelay relay, ResetAcknowledger resetAcknowledger) {
        return new CatalogFeed(catalogSnapshotConsumer, catalogReplica, new CatalogFeed.ResetParticipant() {
            @Override
            public long currentEpoch() {
                return relay.epoch();
            }

            @Override
            public void resetTo(long epoch, CatalogSnapshot snapshot) {
                relay.reset(epoch);
            }
        }, resetAcknowledger);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startCatalogFeed(ApplicationReadyEvent event) {
        event.getApplicationContext().getBean(CatalogFeed.class).start();
    }
}
