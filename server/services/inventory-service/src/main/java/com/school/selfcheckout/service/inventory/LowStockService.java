package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Dtos.LowStockAlert;
import com.school.selfcheckout.contract.Dtos.LowStockResponse;
import com.school.selfcheckout.data.inventory.InventoryRepository;
import com.school.selfcheckout.data.inventory.LowStockRow;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Low-stock reporting -- the read side of inventory.
 *
 * The write side (decrements, alerts, shortfalls) runs inside checkout-service,
 * because it must commit atomically with completing the transaction. Both sides
 * use the same inventory-data library: this is the coupling-at-the-data-layer
 * a service-based architecture accepts, in place of one service calling the
 * other.
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
