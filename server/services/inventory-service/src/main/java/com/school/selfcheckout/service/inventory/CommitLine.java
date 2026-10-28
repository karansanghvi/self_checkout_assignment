package com.school.selfcheckout.service.inventory;

import java.util.UUID;

/** One line of one applied transaction: what it asked for, and what stock allowed. */
public record CommitLine(UUID transactionId, String sku, int requested, int fulfilled) {

    public boolean isShort() {
        return fulfilled < requested;
    }
}
