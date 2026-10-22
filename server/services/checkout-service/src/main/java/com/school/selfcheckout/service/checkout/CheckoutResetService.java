package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * checkout-service's part of a reset: empty checkout_db, move the outbox to
 * the new epoch, and re-fetch the catalog replica.
 *
 * admin-service calls this last, after analytics-service has moved to the
 * new epoch, so every scan event published from here on is one analytics is
 * already waiting for.
 */
@Service
public class CheckoutResetService {

    private static final Logger log = LoggerFactory.getLogger(CheckoutResetService.class);

    private final OutboxRelay relay;
    private final CatalogReplica catalog;

    public CheckoutResetService(OutboxRelay relay, CatalogReplica catalog) {
        this.relay = relay;
        this.catalog = catalog;
    }

    public ResetAck reset(ResetCommand command) {
        long start = System.nanoTime();
        catalog.reload();
        relay.reset(command.epoch());
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset to epoch {} in {} ms", command.epoch(), millis);
        return new ResetAck("checkout-service", command.epoch(), millis);
    }
}
