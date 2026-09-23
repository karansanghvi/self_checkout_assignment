package com.school.selfcheckout.domain;

import java.time.Instant;

/** A SKU currently under the low-stock threshold. {@code triggeredAt} is null if it never crossed the configured one. */
public record LowStockRow(String sku, String name, int stock, Instant triggeredAt) {
}
