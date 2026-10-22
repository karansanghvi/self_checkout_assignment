package com.school.selfcheckout.service.catalog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Catalog service: {@code GET /items}, and sole owner of catalog_db.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the shared
 * error handler from the common library. No other service's classes are on
 * the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class CatalogServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CatalogServiceApplication.class, args);
    }
}
