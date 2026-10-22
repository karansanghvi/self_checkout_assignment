package com.school.selfcheckout.service.analytics;

/**
 * analytics_db's record of the current run: its epoch, and the topic offset
 * at which that epoch's scan events start. A restarted analytics-service
 * replays the topic from {@code startOffset} to rebuild its window.
 */
public record AnalyticsState(long epoch, long startOffset) {
}
