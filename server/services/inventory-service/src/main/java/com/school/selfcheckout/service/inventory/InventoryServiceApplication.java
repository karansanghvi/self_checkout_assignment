package com.school.selfcheckout.service.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Inventory service: stock -- the commit step of the completion saga, and low-stock reporting.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the shared
 * error handler from the common library. No other service's classes are on
 * the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
