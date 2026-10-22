package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.InternalHttp;
import com.school.selfcheckout.contract.InternalDtos.StockCommitLine;
import com.school.selfcheckout.contract.InternalDtos.StockCommitRequest;
import com.school.selfcheckout.contract.InternalDtos.StockCommitResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Step 2 of the completion saga: asks inventory-service to commit a basket's
 * stock.
 *
 * Retries are safe only because inventory's endpoint is idempotent on the
 * transaction id. After a read timeout checkout cannot tell whether the
 * request was lost or only its response was, and with idempotency it does not
 * need to: sending it again either applies the basket or returns the result
 * of the attempt that already did.
 *
 * Failures are split by whether retrying could help:
 * <ul>
 *   <li>{@link Unavailable} -- connection refused, timeout, 5xx. Retried here
 *       with backoff; if it persists, the saga is left to be resumed.</li>
 *   <li>{@link Rejected} -- a 4xx. Inventory understood and refused;
 *       resending the same request cannot change that. Not retried.</li>
 * </ul>
 */
@Component
public class InventoryClient {

    private final RestClient inventory;
    private final int maxAttempts;
    private final Duration backoff;

    @Autowired
    public InventoryClient(@Value("${app.inventory.url}") String inventoryUrl,
                           @Value("${app.inventory.connect-timeout-ms:1000}") long connectTimeoutMs,
                           @Value("${app.inventory.read-timeout-ms:3000}") long readTimeoutMs,
                           @Value("${app.inventory.max-attempts:3}") int maxAttempts,
                           @Value("${app.inventory.backoff-ms:100}") long backoffMs) {
        this(InternalHttp.restClient(inventoryUrl,
                        Duration.ofMillis(connectTimeoutMs), Duration.ofMillis(readTimeoutMs)),
                maxAttempts, Duration.ofMillis(backoffMs));
    }

    InventoryClient(RestClient inventory, int maxAttempts, Duration backoff) {
        this.inventory = inventory;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.backoff = backoff;
    }

    public StockCommitResult commit(UUID transactionId, List<TransactionLine> lines) {
        StockCommitRequest request = new StockCommitRequest(
                transactionId.toString(),
                lines.stream().map(l -> new StockCommitLine(l.sku(), l.quantity())).toList());

        RestClientException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return inventory.post()
                        .uri("/internal/stock-commits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(request)
                        .retrieve()
                        .body(StockCommitResult.class);
            } catch (HttpClientErrorException e) {
                throw new Rejected(e.getStatusCode().value() + " " + e.getResponseBodyAsString());
            } catch (RestClientException e) {
                last = e;
                if (attempt < maxAttempts) {
                    sleep(backoff.multipliedBy(attempt));
                }
            }
        }
        throw new Unavailable(last == null ? "no attempt made" : last.getMessage());
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Unavailable("interrupted while backing off");
        }
    }

    /** Inventory could not be reached, or failed; the commit may or may not have been applied. */
    public static class Unavailable extends RuntimeException {
        public Unavailable(String message) {
            super(message);
        }
    }

    /** Inventory refused the commit, and applied nothing. */
    public static class Rejected extends RuntimeException {
        public Rejected(String message) {
            super(message);
        }
    }
}
