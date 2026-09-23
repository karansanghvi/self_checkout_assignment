package com.school.selfcheckout.catalog;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.CatalogResponse;
import com.school.selfcheckout.domain.Item;
import com.school.selfcheckout.repository.ItemRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

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
 */
@Service
public class CatalogService {

    private static final Logger log = LoggerFactory.getLogger(CatalogService.class);

    private final ItemRepository itemRepository;
    private final AtomicReference<Snapshot> snapshotRef = new AtomicReference<>();

    public CatalogService(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    /** Ordered list plus by-SKU index, rebuilt atomically as one unit. */
    private record Snapshot(List<Item> ordered, Map<String, Item> bySku, CatalogResponse response) {
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
        List<CatalogItem> dtoItems = ordered.stream()
                .map(i -> new CatalogItem(i.sku(), i.name(), i.price()))
                .toList();
        snapshotRef.set(new Snapshot(ordered, Map.copyOf(bySku), new CatalogResponse(dtoItems)));
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

    public CatalogResponse catalogResponse() {
        return snapshot().response();
    }

    /** @return the item, or {@code null} if the SKU is not in the catalog. */
    public Item findBySku(String sku) {
        return sku == null ? null : snapshot().bySku().get(sku);
    }

    public int size() {
        return snapshot().ordered().size();
    }
}
