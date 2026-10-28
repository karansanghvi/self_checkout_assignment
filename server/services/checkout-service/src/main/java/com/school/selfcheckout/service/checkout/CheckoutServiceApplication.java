package com.school.selfcheckout.service.checkout;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Checkout service: the transaction lifecycle, and publisher of the scan and completion events.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the shared
 * error handler from the common library. No other service's classes are on
 * the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class CheckoutServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CheckoutServiceApplication.class, args);
    }
}
