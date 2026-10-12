package com.school.selfcheckout.service.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Analytics service: hopping-window popularity, fed by the scan event log.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the beans in the
 * libraries on its classpath -- common and the data libraries this module
 * declares. Nothing else is on the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class AnalyticsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServiceApplication.class, args);
    }
}
