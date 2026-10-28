package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.Receipt;
import com.school.selfcheckout.contract.Dtos.ReceiptLine;
import com.school.selfcheckout.contract.Dtos.ScanResult;
import com.school.selfcheckout.contract.Dtos.TransactionResponse;
import com.school.selfcheckout.domain.CheckoutException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final CatalogReplica catalog;

    public TransactionService(TransactionRepository transactionRepository, CatalogReplica catalog) {
        this.transactionRepository = transactionRepository;
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
                row.status(),
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
     * Completes the transaction and returns the receipt.
     *
     * One local database transaction, with no call to any other service: the
     * sale and its {@code TransactionCompleted} event are committed together
     * (see {@link TransactionRepository#complete}), and inventory-service
     * takes the stock out when it consumes that event. Nothing inventory
     * could say would change the answer -- under the clamp-and-audit policy
     * it never refuses a basket, and the receipt holds nothing it owns -- so
     * there is nothing to wait for.
     */
    public Receipt complete(String transactionId) {
        UUID id = parseId(transactionId);

        Completion completion = transactionRepository.complete(id);
        if (completion == null) {
            TransactionRow current = transactionRepository.findById(id);
            if (current == null) {
                throw CheckoutException.transactionNotFound(transactionId);
            }
            if (!"OPEN".equals(current.status())) {
                throw CheckoutException.transactionNotOpen(transactionId, current.status());
            }
            if (current.itemCount() == 0) {
                throw CheckoutException.emptyBasket(transactionId);
            }
            // The basket's first scan committed between the claim and the re-read.
            completion = transactionRepository.complete(id);
            if (completion == null) {
                throw notFoundOrNotOpen(id, transactionId);
            }
        }
        return receipt(transactionId, completion);
    }

    private Receipt receipt(String transactionId, Completion completion) {
        TransactionRow tx = completion.transaction();
        List<ReceiptLine> lines = completion.lines().stream()
                .map(line -> new ReceiptLine(line.sku(), nameOf(line.sku()), line.unitPrice(), line.quantity()))
                .toList();
        return new Receipt(
                transactionId,
                tx.stationId(),
                tx.itemCount(),
                tx.runningTotal(),
                tx.startedAt(),
                tx.completedAt() != null ? tx.completedAt() : Instant.now(),
                lines);
    }

    /**
     * The sale is already committed by the time the receipt is built, so a
     * missing name must not turn it into an error: if no catalog snapshot has
     * arrived since a restart, the receipt names the SKU instead.
     */
    private String nameOf(String sku) {
        try {
            CatalogItem item = catalog.findBySku(sku);
            return item != null ? item.name() : sku;
        } catch (CheckoutException e) {
            return sku;
        }
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
        return CheckoutException.transactionNotOpen(rawId, status);
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
