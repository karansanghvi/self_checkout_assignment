package com.school.selfcheckout.service.inventory;

/** What one batch did to one SKU's stock. {@code before - after} units were taken. */
public record StockChange(String sku, int before, int after) {
}
