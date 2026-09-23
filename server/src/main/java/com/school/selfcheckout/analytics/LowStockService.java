package com.school.selfcheckout.analytics;

import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.contract.Dtos.LowStockAlert;
import com.school.selfcheckout.contract.Dtos.LowStockResponse;
import com.school.selfcheckout.domain.LowStockRow;
import com.school.selfcheckout.repository.InventoryRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Low-stock reporting -- the read side of inventory.
 *
 * Extracted from the former {@code InventoryService}, which mixed this
 * reporting query with the stock decrements on the checkout write path.
 * Sharing {@link InventoryRepository} with
 * {@code transactions.InventoryService} is intentional: two layers may read
 * the same tables, they just must not share a service.
 */
@Service
public class LowStockService {

    private final InventoryRepository inventoryRepository;
    private final AppProperties properties;

    public LowStockService(InventoryRepository inventoryRepository, AppProperties properties) {
        this.inventoryRepository = inventoryRepository;
        this.properties = properties;
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
