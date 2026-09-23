package com.school.selfcheckout.transactions;

import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.domain.DecrementResult;
import com.school.selfcheckout.domain.TransactionLine;
import com.school.selfcheckout.repository.InventoryRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * The stock-mutating half of inventory, and part of the checkout write path.
 *
 * Read-only low-stock reporting used to live here too; it now sits in
 * {@code analytics.LowStockService}. The two shared no logic beyond the
 * configured threshold, and they belong to different layers: this one runs
 * inside the /complete transaction, that one answers a reporting query.
 */
@Service
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final AppProperties properties;

    public InventoryService(InventoryRepository inventoryRepository, AppProperties properties) {
        this.inventoryRepository = inventoryRepository;
        this.properties = properties;
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
        int threshold = properties.getLowStockThreshold();

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
