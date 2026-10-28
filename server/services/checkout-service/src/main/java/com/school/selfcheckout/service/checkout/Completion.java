package com.school.selfcheckout.service.checkout;

import java.util.List;

/** A transaction just completed, with the basket lines its event was built from, ordered by SKU. */
public record Completion(TransactionRow transaction, List<TransactionLine> lines) {
}
