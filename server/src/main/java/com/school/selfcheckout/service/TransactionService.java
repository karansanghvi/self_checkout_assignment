package com.school.selfcheckout.service;

import com.school.selfcheckout.api.ApiException;
import com.school.selfcheckout.api.dto.Dtos.Receipt;
import com.school.selfcheckout.api.dto.Dtos.ReceiptLine;
import com.school.selfcheckout.api.dto.Dtos.ScanResult;
import com.school.selfcheckout.api.dto.Dtos.TransactionResponse;
import com.school.selfcheckout.domain.Item;
import com.school.selfcheckout.domain.TransactionLine;
import com.school.selfcheckout.domain.TransactionRow;
import com.school.selfcheckout.repository.TransactionRepository;
import com.school.selfcheckout.repository.TransactionRepository.ScanTotals;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final InventoryService inventoryService;
    private final CatalogService catalogService;
    private final PopularityService popularityService;

    public TransactionService(TransactionRepository transactionRepository,
                              InventoryService inventoryService,
                              CatalogService catalogService,
                              PopularityService popularityService) {
        this.transactionRepository = transactionRepository;
        this.inventoryService = inventoryService;
        this.catalogService = catalogService;
        this.popularityService = popularityService;
    }

    public TransactionResponse start(String stationId) {
        if (stationId == null || stationId.isBlank()) {
            throw ApiException.invalidRequest("stationId is required");
        }
        UUID id = UUID.randomUUID();
        Instant startedAt = transactionRepository.insertOpen(id, stationId);
        return new TransactionResponse(
                id.toString(), stationId, "OPEN", 0, ZERO_MONEY, startedAt);
    }

    public TransactionResponse get(String transactionId) {
        TransactionRow row = requireExisting(transactionId);
        return new TransactionResponse(
                row.id().toString(),
                row.stationId(),
                row.status(),
                row.itemCount(),
                row.runningTotal(),
                row.startedAt());
    }

    /**
     * Scans one physical unit into the basket.
     *
     * Price comes from the in-memory catalog cache, so the whole operation is
     * one database round trip.
     */
    public ScanResult scan(String transactionId, String sku) {
        UUID id = parseId(transactionId);

        Item item = catalogService.findBySku(sku);
        if (item == null) {
            throw ApiException.unknownSku(sku);
        }

        ScanTotals totals = transactionRepository.recordScan(id, sku, item.price());
        if (totals == null) {
            // Rare path only: work out whether this was a 404 or a 409.
            throw notFoundOrNotOpen(id, transactionId);
        }

        // Popularity is measured in scans, per the contract -- so it advances
        // here, not at completion. Cheap in-memory append; the periodic
        // recompute is handed to a background thread.
        popularityService.recordScan(sku);

        return new ScanResult(
                transactionId, sku, item.name(), item.price(), totals.itemCount(), totals.runningTotal());
    }

    /**
     * Completes the transaction and decrements stock for every scanned unit.
     *
     * Every inventory row lock taken here is held until commit, so the body is
     * kept to the minimum: claim the transaction, read the lines, decrement.
     * Receipt assembly adds no further database work -- item names come from
     * the in-memory catalog cache, and the totals were already returned by the
     * claiming UPDATE.
     */
    @Transactional
    public Receipt complete(String transactionId) {
        UUID id = parseId(transactionId);

        TransactionRow claimed = transactionRepository.claimForCompletion(id);
        if (claimed == null) {
            throw notFoundOrNotOpen(id, transactionId);
        }

        List<TransactionLine> lines = transactionRepository.findLinesOrderedBySku(id);
        if (lines.isEmpty()) {
            // Throwing rolls back the status flip above, leaving the
            // transaction OPEN -- which is what a 409 should mean.
            throw ApiException.emptyBasket(transactionId);
        }

        inventoryService.applyBasket(id, lines);

        return buildReceipt(transactionId, claimed, lines);
    }

    private Receipt buildReceipt(String transactionId, TransactionRow tx, List<TransactionLine> lines) {
        List<ReceiptLine> receiptLines = lines.stream()
                .map(line -> new ReceiptLine(
                        line.sku(),
                        nameOf(line.sku()),
                        line.unitPrice(),
                        line.quantity()))
                .toList();

        return new Receipt(
                transactionId,
                tx.stationId(),
                tx.itemCount(),
                tx.runningTotal(),
                tx.startedAt(),
                tx.completedAt() != null ? tx.completedAt() : Instant.now(),
                receiptLines);
    }

    private String nameOf(String sku) {
        Item item = catalogService.findBySku(sku);
        return item != null ? item.name() : sku;
    }

    private TransactionRow requireExisting(String transactionId) {
        UUID id = parseId(transactionId);
        TransactionRow row = transactionRepository.findById(id);
        if (row == null) {
            throw ApiException.transactionNotFound(transactionId);
        }
        return row;
    }

    private ApiException notFoundOrNotOpen(UUID id, String rawId) {
        String status = transactionRepository.findStatus(id);
        return status == null
                ? ApiException.transactionNotFound(rawId)
                : ApiException.transactionNotOpen(rawId, status);
    }

    /** A malformed id is a 404, not a 500 -- it simply identifies no transaction. */
    private static UUID parseId(String transactionId) {
        try {
            return UUID.fromString(transactionId);
        } catch (IllegalArgumentException e) {
            throw ApiException.transactionNotFound(transactionId);
        }
    }

    private static final java.math.BigDecimal ZERO_MONEY =
            java.math.BigDecimal.ZERO.setScale(2, java.math.RoundingMode.HALF_UP);
}
