package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.Receipt;
import com.school.selfcheckout.contract.Dtos.ScanResult;
import com.school.selfcheckout.contract.Dtos.TransactionResponse;
import com.school.selfcheckout.domain.CheckoutException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final CompletionSaga completionSaga;
    private final CatalogReplica catalog;

    public TransactionService(TransactionRepository transactionRepository,
                              CompletionSaga completionSaga,
                              CatalogReplica catalog) {
        this.transactionRepository = transactionRepository;
        this.completionSaga = completionSaga;
        this.catalog = catalog;
    }

    public TransactionResponse start(String stationId) {
        if (stationId == null || stationId.isBlank()) {
            throw CheckoutException.invalidRequest("stationId is required");
        }
        UUID id = UUID.randomUUID();
        Instant startedAt = transactionRepository.insertOpen(id, stationId);
        return new TransactionResponse(
                id.toString(), stationId, "OPEN", 0, ZERO_MONEY, startedAt);
    }

    public TransactionResponse get(String transactionId) {
        TransactionRow row = requireExisting(transactionId);
        return new TransactionResponse(
                row.id().toString(),
                row.stationId(),
                wireStatus(row.status()),
                row.itemCount(),
                row.runningTotal(),
                row.startedAt());
    }

    /**
     * Scans one physical unit into the basket.
     *
     * Price comes from this service's replica of the catalog, so the whole
     * operation is one round trip to checkout_db and no call to
     * catalog-service. The same statement appends the scan to the outbox,
     * which is how analytics-service hears about it.
     */
    public ScanResult scan(String transactionId, String sku) {
        UUID id = parseId(transactionId);

        CatalogItem item = catalog.findBySku(sku);
        if (item == null) {
            throw CheckoutException.unknownSku(sku);
        }

        ScanTotals totals = transactionRepository.recordScan(id, sku, item.price());
        if (totals == null) {
            // Rare path only: work out whether this was a 404 or a 409.
            throw notFoundOrNotOpen(id, transactionId);
        }

        return new ScanResult(
                transactionId, sku, item.name(), item.price(), totals.itemCount(), totals.runningTotal());
    }

    /**
     * Completes the transaction: step 1 of the saga here, the rest in
     * {@link CompletionSaga}.
     *
     * A transaction already in COMPLETING is an earlier completion that did
     * not finish -- typically inventory-service was unreachable and the
     * client got a 503. Calling /complete again resumes that saga rather
     * than refusing it. Inventory's idempotency makes this safe even if the
     * earlier attempt did reach it.
     */
    public Receipt complete(String transactionId) {
        UUID id = parseId(transactionId);

        TransactionRow claimed = transactionRepository.claimForCompletion(id);
        if (claimed == null) {
            TransactionRow current = transactionRepository.findById(id);
            if (current == null) {
                throw CheckoutException.transactionNotFound(transactionId);
            }
            switch (current.status()) {
                case "COMPLETING" -> claimed = current;
                case "OPEN" -> {
                    if (current.itemCount() == 0) {
                        throw CheckoutException.emptyBasket(transactionId);
                    }
                    // A rejected saga reopened it between our two reads; it is fair game again.
                    claimed = transactionRepository.claimForCompletion(id);
                    if (claimed == null) {
                        throw CheckoutException.transactionBeingCompleted(transactionId);
                    }
                }
                default -> throw CheckoutException.transactionNotOpen(transactionId, current.status());
            }
        }

        return completionSaga.drive(claimed);
    }

    /**
     * COMPLETING is internal saga state. The contract's status enum is
     * OPEN | COMPLETED | CANCELLED, and until the saga finishes the basket
     * has not been sold, so it is reported as OPEN.
     */
    private static String wireStatus(String status) {
        return "COMPLETING".equals(status) ? "OPEN" : status;
    }

    private TransactionRow requireExisting(String transactionId) {
        UUID id = parseId(transactionId);
        TransactionRow row = transactionRepository.findById(id);
        if (row == null) {
            throw CheckoutException.transactionNotFound(transactionId);
        }
        return row;
    }

    private CheckoutException notFoundOrNotOpen(UUID id, String rawId) {
        String status = transactionRepository.findStatus(id);
        if (status == null) {
            return CheckoutException.transactionNotFound(rawId);
        }
        return "COMPLETING".equals(status)
                ? CheckoutException.transactionBeingCompleted(rawId)
                : CheckoutException.transactionNotOpen(rawId, status);
    }

    /** A malformed id is a 404, not a 500 -- it simply identifies no transaction. */
    private static UUID parseId(String transactionId) {
        try {
            return UUID.fromString(transactionId);
        } catch (IllegalArgumentException e) {
            throw CheckoutException.transactionNotFound(transactionId);
        }
    }

    private static final java.math.BigDecimal ZERO_MONEY =
            java.math.BigDecimal.ZERO.setScale(2, java.math.RoundingMode.HALF_UP);
}
