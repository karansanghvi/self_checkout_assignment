package com.school.selfcheckout.service.inventory;

/** inventory_db's record of the current run, and of how far into transaction-completed it has applied. */
public record InventoryState(long epoch, long nextOffset) {
}
