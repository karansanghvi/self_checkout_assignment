package com.school.selfcheckout.service.checkout;

/** One row of {@code scan_events}, checkout's outbox: a single unit scanned into some basket. */
public record OutboxEvent(long seq, String sku) {
}
