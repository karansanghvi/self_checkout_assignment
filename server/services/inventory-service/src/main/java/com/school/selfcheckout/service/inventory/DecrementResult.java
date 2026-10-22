package com.school.selfcheckout.service.inventory;

/** Outcome of decrementing one SKU: how many units we actually took, and the resulting stock. */
public record DecrementResult(int fulfilled, int newStock) {

    public boolean isShort(int requested) {
        return fulfilled < requested;
    }
}
