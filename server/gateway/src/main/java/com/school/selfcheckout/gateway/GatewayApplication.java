package com.school.selfcheckout.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The single entry point on :8080.
 *
 * The load client takes one {@code --baseUrl}, so something has to stand in
 * front of five services on five ports. This forwards each request to the
 * service that owns its path and does nothing else: no business logic, no
 * database, no knowledge of the contract beyond URL prefixes. It is
 * infrastructure, not a domain service, and the services still never call
 * one another.
 *
 * All routing lives in application.yml.
 */
@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
