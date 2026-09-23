package com.school.selfcheckout.domain;

import java.time.Instant;

public record PopularWindow(
        long id,
        long windowStart,
        long windowEnd,
        int windowSize,
        int slideInterval,
        Instant computedAt) {
}
