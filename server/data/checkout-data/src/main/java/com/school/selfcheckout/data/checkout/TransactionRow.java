package com.school.selfcheckout.data.checkout;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionRow(
        UUID id,
        String stationId,
        String status,
        int itemCount,
        BigDecimal runningTotal,
        Instant startedAt,
        Instant completedAt) {
}
