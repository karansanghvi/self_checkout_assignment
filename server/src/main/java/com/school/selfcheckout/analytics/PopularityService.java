package com.school.selfcheckout.analytics;

import com.school.selfcheckout.analytics.pipeline.PopularityPipeline;
import com.school.selfcheckout.catalog.CatalogService;
import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.contract.Dtos.PopularItem;
import com.school.selfcheckout.contract.Dtos.PopularItemsResponse;
import com.school.selfcheckout.domain.Item;
import com.school.selfcheckout.domain.PopularWindow;
import com.school.selfcheckout.repository.PopularityRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Hopping-window popularity tracking.
 *
 * The contract asks for the top items among the most recent {@code windowSize}
 * scans, recomputed every {@code slideInterval} scans. The computation itself
 * lives in {@link PopularityPipeline} -- a window -> rank -> persist chain of
 * filters on their own threads -- so a scan costs the request thread one
 * non-blocking enqueue. This class is the facade the rest of the application
 * calls, plus the read side, which serves the latest persisted snapshot.
 *
 * Scans are not persisted individually. The load client generates well over a
 * million of them per run, and writing a row per scan purely to recompute a
 * top-10 every 500 scans would dominate the latency numbers while measuring
 * nothing interesting. What gets persisted is each computed snapshot, which is
 * what "persist it and expose it" actually requires.
 */
@Service
public class PopularityService {

    private final PopularityRepository popularityRepository;
    private final CatalogService catalogService;
    private final PopularityPipeline pipeline;
    private final int windowSize;
    private final int slideInterval;

    public PopularityService(PopularityRepository popularityRepository,
                             CatalogService catalogService,
                             PopularityPipeline pipeline,
                             AppProperties properties) {
        this.popularityRepository = popularityRepository;
        this.catalogService = catalogService;
        this.pipeline = pipeline;
        this.windowSize = Math.max(1, properties.getPopularity().getWindowSize());
        this.slideInterval = Math.max(1, properties.getPopularity().getSlideInterval());
    }

    /** Hands one scan to the analytics pipeline. Never blocks the caller. */
    public void recordScan(String sku) {
        pipeline.submit(sku);
    }

    public PopularItemsResponse popularItems(Integer limitOverride) {
        int limit = (limitOverride == null || limitOverride <= 0) ? 10 : limitOverride;

        PopularWindow window = popularityRepository.findLatestWindow();
        if (window == null) {
            // Fewer than slideInterval scans so far: no snapshot exists yet.
            // The contract still requires every field, so report the live
            // sequence position with an empty ranking.
            long seq = currentSequence();
            return new PopularItemsResponse(
                    windowSize, slideInterval, Math.max(0, seq - windowSize), seq,
                    Instant.now(), List.of());
        }

        List<PopularItem> items = popularityRepository.findEntries(window.id(), limit).stream()
                .map(e -> new PopularItem(e.sku(), nameOf(e.sku()), e.scanCount(), e.rank()))
                .toList();

        return new PopularItemsResponse(
                window.windowSize(),
                window.slideInterval(),
                window.windowStart(),
                window.windowEnd(),
                window.computedAt(),
                items);
    }

    public long currentSequence() {
        return pipeline.currentSequence();
    }

    /**
     * Clears the window. Blocks until the reset has passed through every
     * filter, so no snapshot from the previous run is still in flight.
     */
    public void reset() {
        pipeline.reset();
    }

    private String nameOf(String sku) {
        Item item = catalogService.findBySku(sku);
        return item != null ? item.name() : sku;
    }
}
