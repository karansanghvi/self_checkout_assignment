package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.data.checkout.TransactionLine;
import com.school.selfcheckout.data.inventory.DecrementResult;
import com.school.selfcheckout.data.inventory.InventoryRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * The stock-mutating half of inventory, run inside checkout-service.
 *
 * This is the one place the service boundaries had to bend. Decrementing stock
 * is inventory's business, but it has to commit in the same database
 * transaction that marks the basket COMPLETED -- otherwise a crash between the
 * two would either sell without decrementing or decrement without selling. A
 * call to inventory-service cannot join that transaction, so checkout does the
 * decrement itself, through the shared inventory-data library. The SQL that
 * guarantees no overselling lives in that library, written once, rather than
 * being re-implemented here.
 *
 * inventory-service keeps the read side: {@code GET /inventory/low-stock}.
 */
@Service
public class StockService {

    private final InventoryRepository inventoryRepository;
    private final int threshold;

    public StockService(InventoryRepository inventoryRepository,
                        @Value("${app.low-stock-threshold}") int threshold) {
        this.inventoryRepository = inventoryRepository;
        this.threshold = threshold;
    }

    /**
     * Decrements stock for every line in a completed basket.
     *
     * Callers must pass lines already ordered by SKU -- see
     * TransactionRepository#findLinesOrderedBySku for why that ordering is a
     * correctness requirement rather than a nicety.
     *
     * Runs inside the caller's transaction, so every inventory row lock taken
     * here is held until that transaction commits. Nothing slow belongs in
     * this loop.
     */
    public void applyBasket(UUID transactionId, List<TransactionLine> lines) {
        for (TransactionLine line : lines) {
            DecrementResult result = inventoryRepository.decrement(line.sku(), line.quantity());

            if (result.isShort(line.quantity())) {
                inventoryRepository.recordShortfall(
                        transactionId, line.sku(), line.quantity(), result.fulfilled());
            }

            // Crossing detection without an extra query: the pre-update stock
            // is simply the new stock plus whatever we just took.
            int previousStock = result.newStock() + result.fulfilled();
            if (previousStock >= threshold && result.newStock() < threshold) {
                inventoryRepository.recordLowStockAlert(line.sku(), result.newStock(), threshold);
            }
        }
    }
}
