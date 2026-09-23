package com.school.selfcheckout.service;

import com.school.selfcheckout.api.dto.Dtos.LowStockAlert;
import com.school.selfcheckout.api.dto.Dtos.LowStockResponse;
import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.domain.LowStockRow;
import com.school.selfcheckout.domain.TransactionLine;
import com.school.selfcheckout.repository.InventoryRepository;
import com.school.selfcheckout.repository.InventoryRepository.DecrementResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

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

    public LowStockResponse lowStock(Integer thresholdOverride) {
        int threshold = thresholdOverride != null ? thresholdOverride : properties.getLowStockThreshold();
        Instant generatedAt = Instant.now();

        List<LowStockAlert> alerts = inventoryRepository.findBelowThreshold(threshold).stream()
                .map(row -> toAlert(row, threshold, generatedAt))
                .toList();

        return new LowStockResponse(threshold, generatedAt, alerts);
    }

    private static LowStockAlert toAlert(LowStockRow row, int threshold, Instant fallback) {
        return new LowStockAlert(
                row.sku(),
                row.name(),
                row.stock(),
                threshold,
                row.triggeredAt() != null ? row.triggeredAt() : fallback);
    }
}
