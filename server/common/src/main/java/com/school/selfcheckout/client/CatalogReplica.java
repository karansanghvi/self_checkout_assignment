package com.school.selfcheckout.client;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.domain.CheckoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A read-only, in-memory copy of catalog-service's items, for services that
 * need prices or names but do not own them.
 *
 * In week 5 this fetched {@code GET /items} from catalog-service at startup
 * and on every reset. It is now fed by events: {@link
 * com.school.selfcheckout.messaging.CatalogFeed} reads the latest snapshot
 * from the compacted {@code catalog-snapshots} topic and swaps it in. No
 * service calls catalog-service any more, and none needs it to be up in
 * order to start -- only the topic.
 *
 * The catalog does not change during a run, so between snapshots the copy is
 * exact, and a scan is priced with a hash lookup.
 *
 * Deliberately not a Spring component, so catalog-service itself -- which
 * scans this package too -- does not end up with a replica of its own data.
 * Services that want one declare it as a bean.
 */
public class CatalogReplica {

    private static final Logger log = LoggerFactory.getLogger(CatalogReplica.class);

    private final AtomicReference<Map<String, CatalogItem>> bySku = new AtomicReference<>();

    /** Swaps in a whole catalog atomically. */
    public void apply(List<CatalogItem> items) {
        Map<String, CatalogItem> index = new HashMap<>(Math.max(16, items.size() * 2));
        for (CatalogItem item : items) {
            index.put(item.sku(), item);
        }
        bySku.set(Map.copyOf(index));
        log.info("Catalog replica loaded: {} items", items.size());
    }

    public boolean isLoaded() {
        return bySku.get() != null;
    }

    /**
     * @return the item, or {@code null} if the SKU is not in the catalog
     * @throws CheckoutException DEPENDENCY_FAILED if no catalog snapshot has arrived yet
     */
    public CatalogItem findBySku(String sku) {
        Map<String, CatalogItem> current = bySku.get();
        if (current == null) {
            throw CheckoutException.dependencyFailed(
                    "No catalog snapshot received yet from the catalog-snapshots topic");
        }
        return sku == null ? null : current.get(sku);
    }
}
