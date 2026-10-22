package com.school.selfcheckout.contract;

import java.util.List;

/**
 * Service-to-service DTOs for the {@code /internal/**} endpoints.
 *
 * These are not in the OpenAPI contract and the gateway does not route them;
 * only the services call them. They sit next to {@link Dtos} for the same
 * reason: one file per wire format, diffable in one place.
 */
public final class InternalDtos {

    private InternalDtos() {
    }

    // --- stock commit: checkout-service -> inventory-service ---------------

    /** One basket line to take out of stock. */
    public record StockCommitLine(String sku, int quantity) {
    }

    /**
     * Step 2 of the completion saga. {@code transactionId} is the idempotency
     * key: inventory applies a given transaction's lines at most once, however
     * many times this is sent.
     */
    public record StockCommitRequest(String transactionId, List<StockCommitLine> lines) {
    }

    /** What inventory actually did for one line. {@code fulfilled < requested} means stock ran out. */
    public record StockCommitOutcome(String sku, int requested, int fulfilled) {
    }

    /** {@code replayed} is true when this transaction had already been committed and nothing changed now. */
    public record StockCommitResult(String transactionId, boolean replayed, List<StockCommitOutcome> lines) {
    }

    // --- reset: admin-service -> every other service ------------------------

    /**
     * Wipe run state and start {@code epoch}. Each service reads only the
     * sizes it cares about: catalog the catalog size, inventory the stock.
     */
    public record ResetCommand(long epoch, int catalogSize, int stockPerItem) {
    }

    public record ResetAck(String service, long epoch, long elapsedMs) {
    }
}
