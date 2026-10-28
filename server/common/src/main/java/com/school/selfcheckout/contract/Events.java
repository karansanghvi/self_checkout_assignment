package com.school.selfcheckout.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.school.selfcheckout.contract.Dtos.CatalogItem;

import java.time.Instant;
import java.util.List;

/**
 * Every event that crosses a service boundary, as it appears on Kafka.
 *
 * Since week 6 these are the <i>only</i> way services talk to each other: the
 * internal HTTP endpoints of week 5 ({@code /internal/stock-commits},
 * {@code /internal/reset}, and catalog replication over {@code GET /items})
 * are gone. One file per wire format, as with {@link Dtos}, so a change to
 * the event contract is one diff.
 *
 * <pre>
 *   topic                   event                 producer    consumers
 *   scan-events             ScanRecorded          checkout    analytics
 *   transaction-completed   TransactionCompleted  checkout    inventory
 *   catalog-snapshots       CatalogSnapshot       catalog     checkout, inventory, analytics
 *   reset-requested         ResetRequested        admin       catalog
 *   reset-completed         ResetCompleted        all four    admin
 * </pre>
 */
public final class Events {

    private Events() {
    }

    /**
     * The JSON wrapper every record value carries.
     *
     * @param eventId    unique per event; for outbox events {@code epoch-seq}
     * @param type       the payload's record name, e.g. {@code ScanRecorded}
     * @param epoch      the reset epoch the producer was in. Consumers drop
     *                   events from an epoch older than their own, which is
     *                   what makes a reset clean while events are in flight.
     * @param seq        the producer's outbox sequence number, for consumers
     *                   that must detect redelivery by position; 0 if the
     *                   event did not come from an outbox
     * @param occurredAt when the state change happened, not when it was sent
     * @param payload    one of the records below
     */
    public record Envelope(String eventId, String type, long epoch, long seq, Instant occurredAt, JsonNode payload) {
    }

    // --- checkout -> analytics ---------------------------------------------

    /** One unit scanned into some basket. Popularity counts these, not purchases. */
    public record ScanRecorded(String sku) {
    }

    // --- checkout -> inventory ---------------------------------------------

    public record CompletedLine(String sku, int quantity) {
    }

    /**
     * A basket was sold. checkout has already answered the client; inventory
     * takes the stock out when it consumes this. {@code transactionId} is
     * inventory's idempotency key.
     */
    public record TransactionCompleted(String transactionId, String stationId, List<CompletedLine> lines) {
    }

    // --- admin -> catalog --------------------------------------------------

    /** Start a new run under the envelope's epoch. */
    public record ResetRequested(int catalogSize, int stockPerItem) {
    }

    // --- catalog -> everyone -----------------------------------------------

    /**
     * The whole catalog as of the envelope's epoch, in SKU order. Published
     * to a compacted topic under a single key, so the topic always holds the
     * latest one and a starting service reads it instead of calling
     * catalog-service.
     *
     * A snapshot with a newer epoch than a service has seen is also that
     * service's cue to reset: the reset chain runs admin -> catalog ->
     * everyone else, so the others can never reseed from a stale catalog.
     * {@code stockPerItem} is carried through from {@link ResetRequested}
     * for inventory's sake.
     */
    public record CatalogSnapshot(int stockPerItem, List<CatalogItem> items) {
    }

    // --- everyone -> admin -------------------------------------------------

    /** One service's answer to a reset: done, or failed and why. */
    public record ResetCompleted(String service, boolean ok, String error, long elapsedMs) {
    }
}
