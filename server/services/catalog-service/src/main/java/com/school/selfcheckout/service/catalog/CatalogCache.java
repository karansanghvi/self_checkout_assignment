package com.school.selfcheckout.service.catalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory cache of the catalog.
 *
 * The catalog is 2,000 immutable rows that are never written after seeding, so
 * holding it in memory turns the per-scan price lookup from a database round
 * trip into a hash lookup. Since scans are ~91% of all requests under the load
 * client's workload, this is the single biggest win available on the hot path.
 *
 * This is catalog-service's own cache of its own table. Other services do not
 * read {@code items} -- they cannot, it is in catalog_db -- but replicate it
 * through {@code GET /items} into a {@code CatalogReplica}. After a reset
 * {@link CatalogResetService} reloads this cache before acknowledging, so by
 * the time those replicas re-fetch, they get the new catalog.
 */
@Component
public class CatalogCache {

    private static final Logger log = LoggerFactory.getLogger(CatalogCache.class);

    private final ItemRepository itemRepository;
    private final AtomicReference<Snapshot> snapshotRef = new AtomicReference<>();

    public CatalogCache(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    /** Ordered list plus by-SKU index, rebuilt atomically as one unit. */
    private record Snapshot(List<Item> ordered, Map<String, Item> bySku) {
    }

    @EventListener(ApplicationReadyEvent.class)
    public void warmUp() {
        reload();
    }

    public synchronized void reload() {
        List<Item> ordered = List.copyOf(itemRepository.findAllOrderedBySku());
        Map<String, Item> bySku = new HashMap<>(Math.max(16, ordered.size() * 2));
        for (Item item : ordered) {
            bySku.put(item.sku(), item);
        }
        snapshotRef.set(new Snapshot(ordered, Map.copyOf(bySku)));
        log.info("Catalog cache loaded: {} items", ordered.size());
    }

    private Snapshot snapshot() {
        Snapshot current = snapshotRef.get();
        if (current == null) {
            synchronized (this) {
                current = snapshotRef.get();
                if (current == null) {
                    reload();
                    current = snapshotRef.get();
                }
            }
        }
        return current;
    }

    /** Every item, ordered by SKU. See {@link ItemRepository#findAllOrderedBySku} for why the order matters. */
    public List<Item> allOrderedBySku() {
        return snapshot().ordered();
    }

    /** @return the item, or {@code null} if the SKU is not in the catalog. */
    public Item findBySku(String sku) {
        return sku == null ? null : snapshot().bySku().get(sku);
    }

    public int size() {
        return snapshot().ordered().size();
    }
}
