package com.school.selfcheckout.service.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Admin service: reset and reseed, and sole owner of the schema migrations.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the beans in the
 * libraries on its classpath -- common and the data libraries this module
 * declares. Nothing else is on the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class AdminServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminServiceApplication.class, args);
    }
}
