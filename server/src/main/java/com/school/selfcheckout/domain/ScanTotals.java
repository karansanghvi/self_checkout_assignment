package com.school.selfcheckout.domain;

import java.math.BigDecimal;

/** Basket totals after one scan, as returned by the recording UPDATE. */
public record ScanTotals(int itemCount, BigDecimal runningTotal) {
}
