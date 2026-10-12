package com.school.selfcheckout.data.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Turns a reset performed by another service into a local Spring event.
 *
 * In the monolith, ResetService called {@code catalogService.reload()}
 * directly. Services cannot call each other, so instead admin-service bumps
 * {@code system_epoch} in its reseed transaction and every service that
 * includes this library polls it. A change becomes an
 * {@link EpochChangedEvent}, which local caches listen for.
 *
 * Polling a one-row table twice a second is negligible load, and bounds how
 * long a cache can trail a reset at about {@code app.epoch-poll-ms}.
 */
@Component
public class EpochWatcher {

    private static final Logger log = LoggerFactory.getLogger(EpochWatcher.class);

    /** -1 until the first successful read, which only sets the baseline. */
    private final AtomicLong lastSeen = new AtomicLong(-1);

    private final EpochRepository epochRepository;
    private final ApplicationEventPublisher events;

    public EpochWatcher(EpochRepository epochRepository, ApplicationEventPublisher events) {
        this.epochRepository = epochRepository;
        this.events = events;
    }

    @Scheduled(fixedDelayString = "${app.epoch-poll-ms:500}")
    public void poll() {
        long current;
        try {
            current = epochRepository.currentEpoch();
        } catch (DataAccessException e) {
            // Typically admin-service has not migrated the schema yet.
            log.debug("Could not read system_epoch: {}", e.getMessage());
            return;
        }

        long previous = lastSeen.getAndSet(current);
        if (previous != -1 && previous != current) {
            log.info("Database reset detected: epoch {} -> {}", previous, current);
            events.publishEvent(new EpochChangedEvent(previous, current));
        }
    }
}
