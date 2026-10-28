package com.school.selfcheckout.service.catalog;

/** catalog_db's record of the current run: its epoch, and the stock level that reset asked for. */
public record CatalogState(long epoch, int stockPerItem) {
}
