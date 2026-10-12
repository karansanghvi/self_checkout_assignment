package com.school.selfcheckout.service.checkout;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Checkout service: the transaction lifecycle, including the stock decrement at completion.
 *
 * Scans all of {@code com.school.selfcheckout} so it picks up the beans in the
 * libraries on its classpath -- common and the data libraries this module
 * declares. Nothing else is on the classpath, so nothing else is found.
 */
@SpringBootApplication(scanBasePackages = "com.school.selfcheckout")
public class CheckoutServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CheckoutServiceApplication.class, args);
    }
}
