package com.school.selfcheckout.messaging;

import com.school.selfcheckout.contract.Events.ResetCompleted;

/**
 * Publishes one service's answer to a reset on {@code reset-completed}, where
 * admin-service is collecting them. Keyed by service name.
 */
public class ResetAcknowledger {

    private final EventPublisher publisher;
    private final String service;

    public ResetAcknowledger(EventPublisher publisher, String service) {
        this.publisher = publisher;
        this.service = service;
    }

    public void ok(long epoch, long elapsedMs) {
        publisher.publish(Topic.RESET_COMPLETED, service, epoch, new ResetCompleted(service, true, null, elapsedMs));
    }

    public void failed(long epoch, String error, long elapsedMs) {
        publisher.publish(Topic.RESET_COMPLETED, service, epoch, new ResetCompleted(service, false, error, elapsedMs));
    }

    public String service() {
        return service;
    }
}
