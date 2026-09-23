package com.school.selfcheckout.admin;

import com.school.selfcheckout.analytics.PopularityService;
import com.school.selfcheckout.catalog.CatalogService;
import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.repository.ResetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/** Wipes run state and reseeds the catalog and inventory, then resyncs in-memory state. */
@Service
public class ResetService {

    private static final Logger log = LoggerFactory.getLogger(ResetService.class);

    private final ResetRepository resetRepository;
    private final CatalogService catalogService;
    private final PopularityService popularityService;
    private final AppProperties properties;

    public ResetService(ResetRepository resetRepository,
                        CatalogService catalogService,
                        PopularityService popularityService,
                        AppProperties properties) {
        this.resetRepository = resetRepository;
        this.catalogService = catalogService;
        this.popularityService = popularityService;
        this.properties = properties;
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
     * -- and could contradict -- {@link AppProperties}.
     */
    public Map<String, Object> reset(Integer catalogSizeOverride, Integer stockPerItemOverride) {
        int catalogSize = catalogSizeOverride != null
                ? catalogSizeOverride : properties.getCatalogSize();
        int stockPerItem = stockPerItemOverride != null
                ? stockPerItemOverride : properties.getStockPerItem();

        long start = System.nanoTime();

        resetRepository.reseed(catalogSize, stockPerItem);

        // In-memory state has to follow the database, or the catalog cache and
        // the popularity window would still describe the previous run.
        catalogService.reload();
        popularityService.reset();

        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset complete: {} items x {} units in {} ms", catalogSize, stockPerItem, millis);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("catalogSize", catalogSize);
        result.put("stockPerItem", stockPerItem);
        result.put("elapsedMs", millis);
        return result;
    }
}
