package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.Receipt;
import com.school.selfcheckout.contract.Dtos.ReceiptLine;
import com.school.selfcheckout.domain.CheckoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * The orchestrated saga behind {@code POST /transactions/{id}/complete}.
 *
 * Completing a basket changes two databases: checkout_db (the transaction is
 * sold) and inventory_db (the stock is gone). No single transaction spans
 * both, so checkout-service orchestrates three local steps:
 *
 * <pre>
 *   1. checkout_db    OPEN -> COMPLETING           TransactionService / claimForCompletion
 *   2. inventory_db   commit the basket's stock    InventoryClient -> POST /internal/stock-commits
 *   3. checkout_db    COMPLETING -> COMPLETED      markCompleted
 * </pre>
 *
 * Step 2 is the pivot. It is idempotent, keyed by the transaction id, so it
 * can be retried any number of times; and under the clamp-and-audit policy it
 * never refuses a basket for lack of stock. Once it has succeeded the saga can
 * only move forward, so failures after step 1 are handled by <i>retrying
 * forward</i>, not by undoing:
 *
 * <ul>
 *   <li>Inventory unreachable or erroring: retried inside InventoryClient; if
 *       it stays down, the client gets a 503 and the transaction stays
 *       COMPLETING. {@link RecoverySweeper} -- or the client calling
 *       /complete again -- resumes it later. The transaction is never left
 *       half-done, and stock is never taken twice.</li>
 *   <li>Inventory definitively rejects the basket (a 4xx, e.g. a SKU it has
 *       no row for): this is the one compensating action. Nothing was
 *       applied in inventory, so checkout moves the transaction back to OPEN
 *       and the client gets a 503 explaining why.</li>
 * </ul>
 *
 * No step holds a database connection while waiting on another service:
 * steps 1 and 3 are single autocommit statements.
 */
@Component
public class CompletionSaga {

    private static final Logger log = LoggerFactory.getLogger(CompletionSaga.class);

    private final TransactionRepository transactionRepository;
    private final InventoryClient inventory;
    private final CatalogReplica catalog;

    public CompletionSaga(TransactionRepository transactionRepository,
                          InventoryClient inventory,
                          CatalogReplica catalog) {
        this.transactionRepository = transactionRepository;
        this.inventory = inventory;
        this.catalog = catalog;
    }

    /**
     * Runs steps 2 and 3 for a transaction already in COMPLETING.
     *
     * @throws CheckoutException DEPENDENCY_FAILED if inventory could not be reached or refused the basket
     */
    public Receipt drive(TransactionRow tx) {
        String rawId = tx.id().toString();
        List<TransactionLine> lines = transactionRepository.findLinesOrderedBySku(tx.id());

        try {
            inventory.commit(tx.id(), lines);
        } catch (InventoryClient.Rejected e) {
            boolean reverted = transactionRepository.revertToOpen(tx.id());
            log.warn("inventory-service rejected the stock commit for {} ({}); reverted to OPEN: {}",
                    rawId, e.getMessage(), reverted);
            throw CheckoutException.dependencyFailed(
                    "inventory-service rejected transaction " + rawId + " (" + e.getMessage()
                            + "); the transaction is OPEN again.");
        } catch (InventoryClient.Unavailable e) {
            log.warn("inventory-service unavailable while completing {}; left COMPLETING for retry: {}",
                    rawId, e.getMessage());
            throw CheckoutException.dependencyFailed(
                    "inventory-service is unavailable; completion of transaction " + rawId
                            + " will be retried.");
        }

        TransactionRow completed = transactionRepository.markCompleted(tx.id());
        if (completed == null) {
            // Another attempt (a concurrent retry, or the sweeper) finished it first.
            completed = transactionRepository.findById(tx.id());
            if (completed == null || !"COMPLETED".equals(completed.status())) {
                throw CheckoutException.transactionNotOpen(
                        rawId, completed == null ? "missing" : completed.status());
            }
        }

        return buildReceipt(rawId, completed, lines);
    }

    private Receipt buildReceipt(String transactionId, TransactionRow tx, List<TransactionLine> lines) {
        List<ReceiptLine> receiptLines = lines.stream()
                .map(line -> new ReceiptLine(
                        line.sku(),
                        nameOf(line.sku()),
                        line.unitPrice(),
                        line.quantity()))
                .toList();

        return new Receipt(
                transactionId,
                tx.stationId(),
                tx.itemCount(),
                tx.runningTotal(),
                tx.startedAt(),
                tx.completedAt() != null ? tx.completedAt() : Instant.now(),
                receiptLines);
    }

    private String nameOf(String sku) {
        CatalogItem item = catalog.findBySku(sku);
        return item != null ? item.name() : sku;
    }
}
