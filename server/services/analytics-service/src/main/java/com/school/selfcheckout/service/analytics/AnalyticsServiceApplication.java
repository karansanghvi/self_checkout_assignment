package com.school.selfcheckout.service.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Analytics service: hopping-window popularity, fed from the scan-events Kafka topic.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the shared
 * error handler from the common library. No other service's classes are on
 * the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class AnalyticsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServiceApplication.class, args);
    }
}
