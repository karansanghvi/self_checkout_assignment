package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import com.school.selfcheckout.domain.CheckoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resets the whole system for a new load-test run, by orchestrating each
 * service's own reset.
 *
 * In week 4 this was one SQL transaction over the shared database. Every
 * service now owns its data, so admin-service owns none -- it holds no
 * database at all -- and instead tells each service, in turn, to reset itself
 * under a new epoch. The order is the protocol:
 *
 * <ol>
 *   <li><b>catalog</b> -- reseeds the items. Goes first because every other
 *       service re-reads the catalog from it.</li>
 *   <li><b>inventory</b> -- one fresh stock row per catalog item.</li>
 *   <li><b>analytics</b> -- empties its windows and notes where in the topic
 *       the new epoch starts. Must come before checkout, so that no event of
 *       the new epoch can be published before analytics is waiting for it.</li>
 *   <li><b>checkout</b> -- empties transactions and the outbox, and moves the
 *       relay to the new epoch. From here on, events carry the new epoch.</li>
 * </ol>
 *
 * The epoch is the reset's wall-clock time in milliseconds. That needs no
 * counter -- admin-service is stateless -- and it increases from one reset to
 * the next, which is all the services compare.
 *
 * If a step fails, the reset stops there and reports which service failed.
 * Re-running it is safe: every step is a full wipe-and-reseed.
 */
@Service
public class ResetService {

    private static final Logger log = LoggerFactory.getLogger(ResetService.class);

    /** A service to reset, in order. */
    record Target(String name, String baseUrl) {
    }

    private final ResetClient resetClient;
    private final List<Target> targets;
    private final int defaultCatalogSize;
    private final int defaultStockPerItem;

    @Autowired
    public ResetService(ResetClient resetClient,
                        @Value("${app.services.catalog-url}") String catalogUrl,
                        @Value("${app.services.inventory-url}") String inventoryUrl,
                        @Value("${app.services.analytics-url}") String analyticsUrl,
                        @Value("${app.services.checkout-url}") String checkoutUrl,
                        @Value("${app.catalog-size:2000}") int defaultCatalogSize,
                        @Value("${app.stock-per-item:10000}") int defaultStockPerItem) {
        this(resetClient,
                List.of(new Target("catalog-service", catalogUrl),
                        new Target("inventory-service", inventoryUrl),
                        new Target("analytics-service", analyticsUrl),
                        new Target("checkout-service", checkoutUrl)),
                defaultCatalogSize, defaultStockPerItem);
    }

    ResetService(ResetClient resetClient, List<Target> targets, int defaultCatalogSize, int defaultStockPerItem) {
        this.resetClient = resetClient;
        this.targets = List.copyOf(targets);
        this.defaultCatalogSize = defaultCatalogSize;
        this.defaultStockPerItem = defaultStockPerItem;
    }

    public Map<String, Object> reset() {
        return reset(null, null);
    }

    /**
     * Resets to the given sizes, falling back to configuration for either
     * argument that is null.
     */
    public Map<String, Object> reset(Integer catalogSizeOverride, Integer stockPerItemOverride) {
        int catalogSize = catalogSizeOverride != null ? catalogSizeOverride : defaultCatalogSize;
        int stockPerItem = stockPerItemOverride != null ? stockPerItemOverride : defaultStockPerItem;
        ResetCommand command = new ResetCommand(System.currentTimeMillis(), catalogSize, stockPerItem);

        long start = System.nanoTime();
        List<ResetAck> acks = new ArrayList<>(targets.size());
        for (Target target : targets) {
            try {
                acks.add(resetClient.reset(target.baseUrl(), command));
            } catch (RestClientException e) {
                log.error("Reset to epoch {} failed at {}: {}", command.epoch(), target.name(), e.getMessage());
                throw CheckoutException.dependencyFailed(
                        "Reset failed at " + target.name() + " after " + acks.size()
                                + " service(s) had reset: " + e.getMessage());
            }
        }
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset complete: {} items x {} units in {} ms, epoch {}",
                catalogSize, stockPerItem, millis, command.epoch());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("catalogSize", catalogSize);
        result.put("stockPerItem", stockPerItem);
        result.put("epoch", command.epoch());
        result.put("elapsedMs", millis);
        result.put("services", acks);
        return result;
    }
}
