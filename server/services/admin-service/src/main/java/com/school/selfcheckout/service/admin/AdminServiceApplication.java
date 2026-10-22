package com.school.selfcheckout.service.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Admin service: orchestrates a system-wide reset. Stateless; owns no database.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the shared
 * error handler from the common library. No other service's classes are on
 * the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class AdminServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminServiceApplication.class, args);
    }
}
