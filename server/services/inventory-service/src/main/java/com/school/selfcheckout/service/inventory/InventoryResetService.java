package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import com.school.selfcheckout.domain.CheckoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * inventory-service's part of a reset: one fresh stock row per catalog item.
 *
 * The SKU list comes from catalog-service, which admin-service has already
 * reset, so the two databases agree on what exists. It is fetched before the
 * database transaction opens, so no HTTP call ever runs while inventory rows
 * are locked.
 */
@Service
public class InventoryResetService {

    private static final Logger log = LoggerFactory.getLogger(InventoryResetService.class);

    private final InventoryRepository inventoryRepository;
    private final CatalogReplica catalog;

    public InventoryResetService(InventoryRepository inventoryRepository, CatalogReplica catalog) {
        this.inventoryRepository = inventoryRepository;
        this.catalog = catalog;
    }

    public ResetAck reset(ResetCommand command) {
        if (command.stockPerItem() < 0) {
            throw CheckoutException.invalidRequest("stockPerItem must not be negative");
        }
        long start = System.nanoTime();
        List<CatalogItem> items = catalog.reload();
        inventoryRepository.reseed(items, command.stockPerItem());
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset to epoch {}: {} SKUs x {} units in {} ms",
                command.epoch(), items.size(), command.stockPerItem(), millis);
        return new ResetAck("inventory-service", command.epoch(), millis);
    }
}
