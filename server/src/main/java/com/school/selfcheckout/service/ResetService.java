package com.school.selfcheckout.service;

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
        return reset(properties.getCatalogSize(), properties.getStockPerItem());
    }

    public Map<String, Object> reset(int catalogSize, int stockPerItem) {
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
