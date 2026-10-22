package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.client.CatalogReplica;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.util.Map;

@Configuration
public class AnalyticsConfiguration {

    /** Item names for the popular-items response, replicated from catalog-service. */
    @Bean
    public CatalogReplica catalogReplica(@Value("${app.catalog-url}") String catalogUrl) {
        return new CatalogReplica(catalogUrl);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void warmUpCatalog(ApplicationReadyEvent event) {
        event.getApplicationContext().getBean(CatalogReplica.class).warmUpInBackground();
    }

    /**
     * No group id and no auto-commit: {@link ScanConsumer} assigns the one
     * partition itself and keeps its position in analytics_db, next to the
     * windows that position produced.
     */
    @Bean
    public Consumer<String, String> scanEventConsumer(@Value("${app.kafka.bootstrap-servers}") String bootstrap) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.CLIENT_ID_CONFIG, "analytics-scan-consumer",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5_000,
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000),
                new StringDeserializer(), new StringDeserializer());
    }
}
