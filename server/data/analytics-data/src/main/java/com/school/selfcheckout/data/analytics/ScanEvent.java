package com.school.selfcheckout.data.analytics;

/** One row of {@code scan_events}: a single unit scanned into some basket. */
public record ScanEvent(long seq, String sku) {
}
