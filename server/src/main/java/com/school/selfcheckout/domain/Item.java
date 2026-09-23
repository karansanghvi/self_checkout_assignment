package com.school.selfcheckout.domain;

import java.math.BigDecimal;

/** An immutable catalog entry. The catalog is never written after seeding. */
public record Item(String sku, String name, BigDecimal price) {
}
