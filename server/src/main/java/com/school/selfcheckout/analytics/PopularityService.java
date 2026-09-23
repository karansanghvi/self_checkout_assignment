package com.school.selfcheckout.analytics;

import com.school.selfcheckout.catalog.CatalogService;
import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.contract.Dtos.PopularItem;
import com.school.selfcheckout.contract.Dtos.PopularItemsResponse;
import com.school.selfcheckout.domain.Item;
import com.school.selfcheckout.domain.PopularEntry;
import com.school.selfcheckout.domain.PopularWindow;
import com.school.selfcheckout.repository.PopularityRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Hopping-window popularity tracking.
 *
 * The contract asks for the top items among the most recent {@code windowSize}
 * scans, recomputed every {@code slideInterval} scans. A ring buffer is the
 * natural shape for that: appends are O(1) and the window is simply whatever
 * the buffer currently holds.
 *
 * Scans are not persisted individually. The load client generates well over a
 * million of them per run, and writing a row per scan purely to recompute a
 * top-10 every 500 scans would dominate the latency numbers while measuring
 * nothing interesting. What gets persisted is each computed snapshot, which is
 * what "persist it and expose it" actually requires.
 *
 * Recomputation runs off the request thread, so scan latency does not spike
 * every 500th scan.
 */
@Service
public class PopularityService {

    private static final Logger log = LoggerFactory.getLogger(PopularityService.class);

    /** Persist more ranks than the default limit=10 so larger queries can be served from a snapshot. */
    private static final int MAX_PERSISTED_RANKS = 50;

    private final PopularityRepository popularityRepository;
    private final CatalogService catalogService;
    private final int windowSize;
    private final int slideInterval;

    private final Object ringLock = new Object();
    private final String[] ring;
    private long scanSequence = 0L;

    private final ThreadPoolExecutor recomputeExecutor;

    public PopularityService(PopularityRepository popularityRepository,
                             CatalogService catalogService,
                             AppProperties properties) {
        this.popularityRepository = popularityRepository;
        this.catalogService = catalogService;
        this.windowSize = Math.max(1, properties.getPopularity().getWindowSize());
        this.slideInterval = Math.max(1, properties.getPopularity().getSlideInterval());
        this.ring = new String[windowSize];

        // Queue depth of 1 plus DiscardOldest: if recomputation ever falls
        // behind the scan rate we skip stale hops rather than building a
        // backlog. Analytics should never apply backpressure to checkout.
        this.recomputeExecutor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1),
                r -> {
                    Thread t = new Thread(r, "popularity-recompute");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    /** Appends one scan to the window, triggering a recompute every slideInterval scans. */
    public void recordScan(String sku) {
        long triggerAt;
        synchronized (ringLock) {
            ring[(int) (scanSequence % windowSize)] = sku;
            scanSequence++;
            triggerAt = (scanSequence % slideInterval == 0) ? scanSequence : -1L;
        }
        if (triggerAt > 0) {
            recomputeExecutor.execute(() -> recompute(triggerAt));
        }
    }

    private void recompute(long windowEnd) {
        try {
            String[] snapshot;
            long sequenceAtCopy;
            synchronized (ringLock) {
                snapshot = ring.clone();
                sequenceAtCopy = scanSequence;
            }

            int valid = (int) Math.min(sequenceAtCopy, windowSize);
            Map<String, Integer> counts = new HashMap<>();
            for (int i = 0; i < valid; i++) {
                String sku = snapshot[i];
                if (sku != null) {
                    counts.merge(sku, 1, Integer::sum);
                }
            }

            List<PopularEntry> entries = new ArrayList<>();
            List<Map.Entry<String, Integer>> ranked = counts.entrySet().stream()
                    // Tie-break by SKU so equal counts rank deterministically.
                    .sorted(Comparator.<Map.Entry<String, Integer>>comparingInt(Map.Entry::getValue).reversed()
                            .thenComparing(Map.Entry::getKey))
                    .limit(MAX_PERSISTED_RANKS)
                    .toList();
            for (int i = 0; i < ranked.size(); i++) {
                entries.add(new PopularEntry(i + 1, ranked.get(i).getKey(), ranked.get(i).getValue()));
            }

            long windowStart = Math.max(0, windowEnd - windowSize);
            popularityRepository.saveWindow(windowStart, windowEnd, windowSize, slideInterval, entries);
        } catch (Exception e) {
            log.warn("Popularity recompute for windowEnd={} failed: {}", windowEnd, e.toString());
        }
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
        synchronized (ringLock) {
            return scanSequence;
        }
    }

    /** Clears the in-memory window. Used by the reset path so runs start clean. */
    public void reset() {
        synchronized (ringLock) {
            java.util.Arrays.fill(ring, null);
            scanSequence = 0L;
        }
    }

    private String nameOf(String sku) {
        Item item = catalogService.findBySku(sku);
        return item != null ? item.name() : sku;
    }

    @PreDestroy
    public void shutdown() {
        recomputeExecutor.shutdown();
    }
}
