package com.school.selfcheckout.service.catalog;

import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import com.school.selfcheckout.domain.CheckoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * catalog-service's part of a reset: reseed catalog_db, then reload the cache.
 *
 * admin-service calls this first, before any other service, because
 * inventory-service reseeds its stock rows from {@code GET /items} and the
 * other services re-fetch their replicas from it.
 */
@Service
public class CatalogResetService {

    private static final Logger log = LoggerFactory.getLogger(CatalogResetService.class);

    private final ItemRepository itemRepository;
    private final CatalogCache cache;

    public CatalogResetService(ItemRepository itemRepository, CatalogCache cache) {
        this.itemRepository = itemRepository;
        this.cache = cache;
    }

    public ResetAck reset(ResetCommand command) {
        if (command.catalogSize() <= 0) {
            throw CheckoutException.invalidRequest("catalogSize must be positive");
        }
        long start = System.nanoTime();
        itemRepository.reseed(command.catalogSize());
        cache.reload();
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset to epoch {}: {} items in {} ms", command.epoch(), cache.size(), millis);
        return new ResetAck("catalog-service", command.epoch(), millis);
    }
}
