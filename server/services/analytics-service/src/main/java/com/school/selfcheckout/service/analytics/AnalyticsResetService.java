package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * analytics-service's part of a reset: re-fetch the catalog replica, then
 * have the scan consumer empty analytics_db and start the new epoch.
 */
@Service
public class AnalyticsResetService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsResetService.class);

    private final ScanConsumer scanConsumer;
    private final CatalogReplica catalog;

    public AnalyticsResetService(ScanConsumer scanConsumer, CatalogReplica catalog) {
        this.scanConsumer = scanConsumer;
        this.catalog = catalog;
    }

    public ResetAck reset(ResetCommand command) {
        long start = System.nanoTime();
        catalog.reload();
        scanConsumer.reset(command.epoch());
        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("Reset to epoch {} in {} ms", command.epoch(), millis);
        return new ResetAck("analytics-service", command.epoch(), millis);
    }
}
