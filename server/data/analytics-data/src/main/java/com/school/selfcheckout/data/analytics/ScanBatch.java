package com.school.selfcheckout.data.analytics;

import java.util.List;

/**
 * One poll of the scan feed: the events after a given seq, plus the epoch they
 * were read under. Both come from a single statement, so they describe the
 * same snapshot -- a batch can never mix events from before and after a reset.
 */
public record ScanBatch(long epoch, List<ScanEvent> events) {
}
