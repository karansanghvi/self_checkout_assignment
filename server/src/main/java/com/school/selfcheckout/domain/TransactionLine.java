package com.school.selfcheckout.domain;

import java.math.BigDecimal;

/** One aggregated basket line: N units of a single SKU. */
public record TransactionLine(String sku, int quantity, BigDecimal unitPrice) {
}
