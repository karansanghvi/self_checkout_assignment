package com.school.selfcheckout.service.catalog;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Events.CatalogSnapshot;
import com.school.selfcheckout.contract.Events.ResetRequested;
import com.school.selfcheckout.messaging.EventPublisher;
import com.school.selfcheckout.messaging.ResetAcknowledger;
import com.school.selfcheckout.messaging.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * catalog-service's part of a reset, and the first link of the reset chain:
 *
 * <pre>
 *   admin --reset-requested--> catalog --catalog-snapshots--> inventory, checkout, analytics
 *                                 |                                     |
 *                                 +--------- reset-completed -----------+--> admin
 * </pre>
 *
 * catalog reseeds catalog_db, reloads its cache, then publishes the new
 * catalog as a snapshot. The other services reset when they see it. So they
 * reseed from the new catalog by construction, without admin-service having
 * to call them in order as it did in week 5.
 */
@Service
public class CatalogResetService {

    private static final Logger log = LoggerFactory.getLogger(CatalogResetService.class);

    /** The single key on the compacted topic, so it keeps exactly the latest snapshot. */
    static final String SNAPSHOT_KEY = "catalog";

    private final ItemRepository itemRepository;
    private final CatalogCache cache;
    private final EventPublisher publisher;
    private final ResetAcknowledger acks;

    public CatalogResetService(ItemRepository itemRepository, CatalogCache cache,
                               EventPublisher publisher, ResetAcknowledger acks) {
        this.itemRepository = itemRepository;
        this.cache = cache;
        this.publisher = publisher;
        this.acks = acks;
    }

    /**
     * Handles one reset request. A request for the epoch catalog is already
     * in is a redelivery: the snapshot is republished and acknowledged again,
     * which is harmless downstream. An older one is ignored.
     */
    public void handle(long epoch, ResetRequested request) {
        long current = itemRepository.loadState().epoch();
        if (epoch < current) {
            log.info("Ignoring reset request for epoch {}; already at {}", epoch, current);
            return;
        }
        long start = System.nanoTime();
        if (epoch > current) {
            if (request.catalogSize() <= 0 || request.stockPerItem() < 0) {
                acks.failed(epoch, "catalogSize must be positive and stockPerItem not negative", 0);
                return;
            }
            itemRepository.reseed(epoch, request.catalogSize(), request.stockPerItem());
            cache.reload();
        }
        publishSnapshot();
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset to epoch {}: {} items in {} ms", epoch, cache.size(), millis);
        acks.ok(epoch, millis);
    }

    /**
     * Republishes the current catalog at startup. This is what fills the
     * topic on the very first run, and it repairs the one crash window that
     * exists: catalog_db reseeded but the snapshot never sent.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void publishOnStartup() {
        try {
            publishSnapshot();
        } catch (RuntimeException e) {
            // Not fatal: the next reset publishes one anyway.
            log.warn("Could not publish the catalog snapshot at startup: {}", e.getMessage());
        }
    }

    void publishSnapshot() {
        CatalogState state = itemRepository.loadState();
        CatalogSnapshot snapshot = new CatalogSnapshot(state.stockPerItem(), cache.allOrderedBySku().stream()
                .map(i -> new CatalogItem(i.sku(), i.name(), i.price()))
                .toList());
        publisher.publish(Topic.CATALOG_SNAPSHOTS, SNAPSHOT_KEY, state.epoch(), snapshot);
        log.info("Published catalog snapshot: epoch {}, {} items", state.epoch(), snapshot.items().size());
    }
}
