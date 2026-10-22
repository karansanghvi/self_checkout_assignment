package com.school.selfcheckout.service.checkout;

import java.util.List;

/** Where the outbox relay sends scans. {@link KafkaScanPublisher} in production, a fake in tests. */
public interface ScanPublisher {

    /**
     * Publishes the events in order and returns only once the broker has
     * acknowledged every one of them.
     *
     * @throws RuntimeException if any event may not have been stored; the
     *         caller then publishes the whole batch again
     */
    void publish(long epoch, List<OutboxEvent> events);
}
