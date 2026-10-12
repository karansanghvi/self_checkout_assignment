package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.contract.Dtos.PopularItem;
import com.school.selfcheckout.contract.Dtos.PopularItemsResponse;
import com.school.selfcheckout.data.analytics.PopularWindow;
import com.school.selfcheckout.data.analytics.PopularityRepository;
import com.school.selfcheckout.data.catalog.CatalogCache;
import com.school.selfcheckout.data.catalog.Item;
import com.school.selfcheckout.service.analytics.pipeline.PopularityPipeline;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Hopping-window popularity: the read side of analytics.
 *
 * The contract asks for the top items among the most recent {@code windowSize}
 * scans, recomputed every {@code slideInterval} scans. The computation lives in
 * {@link PopularityPipeline} -- a window -> rank -> persist chain of filters on
 * their own threads -- fed by {@link ScanFeed} from the {@code scan_events}
 * table that checkout-service writes. This class serves the latest persisted
 * snapshot.
 */
@Service
public class PopularityService {

    private final PopularityRepository popularityRepository;
    private final CatalogCache catalog;
    private final PopularityPipeline pipeline;
    private final int windowSize;
    private final int slideInterval;

    public PopularityService(PopularityRepository popularityRepository,
                             CatalogCache catalog,
                             PopularityPipeline pipeline,
                             @Value("${app.popularity.window-size:1000}") int windowSize,
                             @Value("${app.popularity.slide-interval:500}") int slideInterval) {
        this.popularityRepository = popularityRepository;
        this.catalog = catalog;
        this.pipeline = pipeline;
        this.windowSize = Math.max(1, windowSize);
        this.slideInterval = Math.max(1, slideInterval);
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

    private String nameOf(String sku) {
        Item item = catalog.findBySku(sku);
        return item != null ? item.name() : sku;
    }
}
