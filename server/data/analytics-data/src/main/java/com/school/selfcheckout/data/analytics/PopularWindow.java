package com.school.selfcheckout.data.analytics;

import java.time.Instant;

public record PopularWindow(
        long id,
        long windowStart,
        long windowEnd,
        int windowSize,
        int slideInterval,
        Instant computedAt) {
}
