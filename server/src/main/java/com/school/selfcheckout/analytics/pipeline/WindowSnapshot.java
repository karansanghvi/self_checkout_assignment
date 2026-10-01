package com.school.selfcheckout.analytics.pipeline;

/**
 * The contents of the hopping window at one hop: the SKUs of scans
 * {@code (windowStart, windowEnd]}, in no particular order.
 */
record WindowSnapshot(long windowStart, long windowEnd, String[] skus) {
}
