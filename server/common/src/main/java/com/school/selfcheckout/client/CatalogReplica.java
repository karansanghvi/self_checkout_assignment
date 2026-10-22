package com.school.selfcheckout.client;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.CatalogResponse;
import com.school.selfcheckout.domain.CheckoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A read-only, in-memory copy of catalog-service's items, for services that
 * need prices or names but do not own them.
 *
 * In week 4 every service read the shared {@code items} table. Now only
 * catalog-service has that table, so the others replicate it over the public
 * {@code GET /items} endpoint: once at startup, and again whenever
 * admin-service resets the system. The catalog does not change during a run,
 * so between those two points the copy is exact, and a scan is still priced
 * with a hash lookup rather than a call to catalog-service.
 *
 * Deliberately not a Spring component, so the catalog-service itself -- which
 * scans this package too -- does not end up with a replica of its own data.
 * Services that want one declare it as a bean.
 */
public class CatalogReplica {

    private static final Logger log = LoggerFactory.getLogger(CatalogReplica.class);

    private static final Duration WARM_UP_RETRY = Duration.ofSeconds(1);

    private final RestClient catalog;
    private final AtomicReference<Map<String, CatalogItem>> bySku = new AtomicReference<>();

    public CatalogReplica(String catalogBaseUrl) {
        this(InternalHttp.restClient(catalogBaseUrl, Duration.ofSeconds(1), Duration.ofSeconds(10)));
    }

    CatalogReplica(RestClient catalog) {
        this.catalog = catalog;
    }

    /**
     * Fetches the full catalog and swaps it in atomically.
     *
     * @return the items, in catalog-service's order (by SKU)
     * @throws CheckoutException DEPENDENCY_FAILED if catalog-service cannot be reached
     */
    public List<CatalogItem> reload() {
        List<CatalogItem> items;
        try {
            CatalogResponse response = catalog.get().uri("/items").retrieve().body(CatalogResponse.class);
            items = response == null || response.items() == null ? List.of() : response.items();
        } catch (RestClientException e) {
            throw CheckoutException.dependencyFailed("catalog-service unavailable: " + e.getMessage());
        }
        Map<String, CatalogItem> index = new HashMap<>(Math.max(16, items.size() * 2));
        for (CatalogItem item : items) {
            index.put(item.sku(), item);
        }
        bySku.set(Map.copyOf(index));
        log.info("Catalog replica loaded: {} items", items.size());
        return items;
    }

    /**
     * Loads the replica on a background thread, retrying until catalog-service
     * answers. A service must be able to start while catalog-service is down
     * -- that is part of being independently deployable -- so startup never
     * blocks on this.
     */
    public void warmUpInBackground() {
        Thread t = new Thread(() -> {
            while (bySku.get() == null) {
                try {
                    reload();
                } catch (CheckoutException e) {
                    log.info("Catalog replica not loaded yet ({}); retrying", e.getMessage());
                    try {
                        Thread.sleep(WARM_UP_RETRY.toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "catalog-replica-warm-up");
        t.setDaemon(true);
        t.start();
    }

    /**
     * @return the item, or {@code null} if the SKU is not in the catalog
     * @throws CheckoutException DEPENDENCY_FAILED if the replica has never loaded and catalog-service is still down
     */
    public CatalogItem findBySku(String sku) {
        Map<String, CatalogItem> current = bySku.get();
        if (current == null) {
            synchronized (this) {
                current = bySku.get();
                if (current == null) {
                    reload();
                    current = bySku.get();
                }
            }
        }
        return sku == null ? null : current.get(sku);
    }
}
