package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.data.system.ResetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Wipes run state and reseeds the catalog and inventory.
 *
 * In the monolith this also drained the popularity pipeline and reloaded the
 * catalog cache, by calling those beans directly. Those now live in other
 * services, which admin-service must not call. Instead the reseed bumps
 * {@code system_epoch} in the same transaction, and each service notices and
 * rebuilds its own in-memory state within about half a second:
 *
 * <ul>
 *   <li>catalog-service and checkout-service reload their catalog caches
 *       ({@code EpochWatcher} -> {@code CatalogCache});</li>
 *   <li>analytics-service resets its pipeline ({@code ScanFeed}), and any
 *       window from the old run that it tries to persist is rejected by the
 *       epoch check in {@code PopularityRepository#saveWindow}.</li>
 * </ul>
 */
@Service
public class ResetService {

    private static final Logger log = LoggerFactory.getLogger(ResetService.class);

    private final ResetRepository resetRepository;
    private final int defaultCatalogSize;
    private final int defaultStockPerItem;

    public ResetService(ResetRepository resetRepository,
                        @Value("${app.catalog-size:2000}") int defaultCatalogSize,
                        @Value("${app.stock-per-item:10000}") int defaultStockPerItem) {
        this.resetRepository = resetRepository;
        this.defaultCatalogSize = defaultCatalogSize;
        this.defaultStockPerItem = defaultStockPerItem;
    }

    public Map<String, Object> reset() {
        return reset(null, null);
    }

    /**
     * Resets to the given sizes, falling back to configuration for either
     * argument that is null.
     *
     * Resolving the defaults here rather than at the controller is deliberate:
     * AdminController used to hardcode 2000/10000, which silently duplicated
     * -- and could contradict -- the configuration.
     */
    public Map<String, Object> reset(Integer catalogSizeOverride, Integer stockPerItemOverride) {
        int catalogSize = catalogSizeOverride != null ? catalogSizeOverride : defaultCatalogSize;
        int stockPerItem = stockPerItemOverride != null ? stockPerItemOverride : defaultStockPerItem;

        long start = System.nanoTime();
        long epoch = resetRepository.reseed(catalogSize, stockPerItem);
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset complete: {} items x {} units in {} ms, epoch {}", catalogSize, stockPerItem, millis, epoch);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("catalogSize", catalogSize);
        result.put("stockPerItem", stockPerItem);
        result.put("epoch", epoch);
        result.put("elapsedMs", millis);
        return result;
    }
}
