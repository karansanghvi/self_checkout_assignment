package com.school.selfcheckout.service.analytics;

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
 * analytics-service on Kafka. It consumes {@code scan-events} and
 * {@code catalog-snapshots}, and produces only its reset acknowledgements.
 * It makes no HTTP call to any other service.
 */
@Configuration
public class AnalyticsConfiguration {

    /** Item names for the popular-items response, replicated from the catalog-snapshots topic. */
    @Bean
    public CatalogReplica catalogReplica() {
        return new CatalogReplica();
    }

    /**
     * No group id and no auto-commit: {@link ScanConsumer} assigns the one
     * partition itself and keeps its position in analytics_db, next to the
     * windows that position produced.
     */
    @Bean(destroyMethod = "close")
    public Consumer<String, String> scanEventConsumer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.consumer(bootstrap, "analytics-scan-consumer", 5_000);
    }

    @Bean(destroyMethod = "close")
    public Producer<String, String> analyticsProducer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.producer(bootstrap, "analytics-service");
    }

    @Bean
    public ResetAcknowledger resetAcknowledger(Producer<String, String> analyticsProducer) {
        return new ResetAcknowledger(new EventPublisher(analyticsProducer), "analytics-service");
    }

    @Bean(destroyMethod = "close")
    public Consumer<String, String> catalogSnapshotConsumer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return KafkaClients.consumer(bootstrap, "analytics-catalog-feed", 10);
    }

    /** On a snapshot from a newer epoch: empty analytics_db and start the window over. */
    @Bean(destroyMethod = "stop")
    public CatalogFeed catalogFeed(Consumer<String, String> catalogSnapshotConsumer, CatalogReplica catalogReplica,
                                   ScanConsumer scanConsumer, PopularityRepository repository,
                                   ResetAcknowledger resetAcknowledger) {
        return new CatalogFeed(catalogSnapshotConsumer, catalogReplica, new CatalogFeed.ResetParticipant() {
            @Override
            public long currentEpoch() {
                return repository.loadState().epoch();
            }

            @Override
            public void resetTo(long epoch, CatalogSnapshot snapshot) {
                scanConsumer.reset(epoch);
            }
        }, resetAcknowledger);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startCatalogFeed(ApplicationReadyEvent event) {
        event.getApplicationContext().getBean(CatalogFeed.class).start();
    }
}
