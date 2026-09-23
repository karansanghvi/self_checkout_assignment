package com.school.selfcheckout.contract;

import jakarta.validation.constraints.NotBlank;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Wire DTOs, mirroring spec/self-checkout-openapi.yaml field-for-field.
 *
 * The load client throws on any unexpected status code or missing field, so
 * these records are the contract: do not rename or reorder fields without
 * re-reading the spec.
 *
 * Lives in its own leaf package rather than under {@code api} so that the
 * layers which assemble these responses -- transactions, analytics, catalog --
 * do not have to import the API layer to do it. Nothing here depends on
 * anything else in the application.
 */
public final class Dtos {

    private Dtos() {
    }

    // --- catalog ---------------------------------------------------------

    public record CatalogItem(String sku, String name, BigDecimal price) {
    }

    public record CatalogResponse(List<CatalogItem> items) {
    }

    // --- transactions ----------------------------------------------------

    public record StartTransactionRequest(@NotBlank String stationId) {
    }

    public record TransactionResponse(
            String transactionId,
            String stationId,
            String status,
            int itemCount,
            BigDecimal runningTotal,
            Instant startedAt) {
    }

    public record ScanItemRequest(@NotBlank String sku) {
    }

    public record ScanResult(
            String transactionId,
            String sku,
            String name,
            BigDecimal unitPrice,
            int itemCount,
            BigDecimal runningTotal) {
    }

    public record ReceiptLine(String sku, String name, BigDecimal unitPrice, int quantity) {
    }

    public record Receipt(
            String transactionId,
            String stationId,
            int itemCount,
            BigDecimal totalAmount,
            Instant startedAt,
            Instant completedAt,
            List<ReceiptLine> lines) {
    }

    // --- inventory -------------------------------------------------------

    public record LowStockAlert(
            String sku,
            String name,
            int currentStock,
            int threshold,
            Instant triggeredAt) {
    }

    public record LowStockResponse(int threshold, Instant generatedAt, List<LowStockAlert> alerts) {
    }

    // --- analytics -------------------------------------------------------

    public record PopularItem(String sku, String name, int scanCount, int rank) {
    }

    public record PopularItemsResponse(
            int windowSize,
            int slideInterval,
            long windowStart,
            long windowEnd,
            Instant computedAt,
            List<PopularItem> items) {
    }

    // --- errors ----------------------------------------------------------

    public record ApiError(String error, String message) {
    }
}
