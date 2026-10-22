package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Dtos.LowStockAlert;
import com.school.selfcheckout.contract.Dtos.LowStockResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Low-stock reporting -- the read side of inventory.
 *
 * The write side (decrements, alerts, shortfalls) is {@link StockCommitService}.
 * Both now live in this service, next to the only database that holds stock.
 */
@Service
public class LowStockService {

    private final InventoryRepository inventoryRepository;
    private final int defaultThreshold;

    public LowStockService(InventoryRepository inventoryRepository,
                           @Value("${app.low-stock-threshold}") int defaultThreshold) {
        this.inventoryRepository = inventoryRepository;
        this.defaultThreshold = defaultThreshold;
    }

    public LowStockResponse lowStock(Integer thresholdOverride) {
        int threshold = thresholdOverride != null ? thresholdOverride : defaultThreshold;
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
