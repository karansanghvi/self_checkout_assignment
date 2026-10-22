package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.InternalDtos.StockCommitLine;
import com.school.selfcheckout.contract.InternalDtos.StockCommitOutcome;
import com.school.selfcheckout.contract.InternalDtos.StockCommitRequest;
import com.school.selfcheckout.contract.InternalDtos.StockCommitResult;
import com.school.selfcheckout.domain.CheckoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The stock-mutating half of inventory: step 2 of checkout-service's
 * completion saga, served at {@code POST /internal/stock-commits}.
 *
 * In week 4 this class lived in checkout-service and ran inside checkout's
 * database transaction, through a shared library. That was the one place the
 * service boundary bent. With a database per service it cannot: checkout's
 * transaction cannot reach inventory_db. So the decrement moved home, and the
 * atomicity checkout used to get from one transaction it now gets from two
 * properties of this endpoint:
 *
 * <ul>
 *   <li><b>All or nothing.</b> Every line of one basket is applied in one
 *       inventory_db transaction. If any line fails, none is applied.</li>
 *   <li><b>Idempotent.</b> The transaction id is the key. A retry -- after a
 *       timeout, say, where checkout cannot know whether the first attempt
 *       landed -- returns the original outcome and changes nothing. That is
 *       what makes it safe for checkout to retry, and to resume a saga after
 *       a crash.</li>
 * </ul>
 *
 * Under the clamp-and-audit policy this never rejects a basket for lack of
 * stock: a short line is fulfilled as far as stock allows and the shortfall
 * recorded, since the contract gives {@code /complete} no out-of-stock error.
 */
@Service
public class StockCommitService {

    private final InventoryRepository inventoryRepository;
    private final int threshold;

    public StockCommitService(InventoryRepository inventoryRepository,
                              @Value("${app.low-stock-threshold}") int threshold) {
        this.inventoryRepository = inventoryRepository;
        this.threshold = threshold;
    }

    @Transactional
    public StockCommitResult commit(StockCommitRequest request) {
        UUID transactionId = parseId(request.transactionId());
        Map<String, Integer> lines = mergeAndSort(request.lines());

        if (!inventoryRepository.claimCommit(transactionId)) {
            return new StockCommitResult(
                    request.transactionId(), true, inventoryRepository.findCommitLines(transactionId));
        }

        // Every inventory row lock taken here is held until commit, so nothing
        // slow belongs in this loop.
        List<StockCommitOutcome> outcomes = new ArrayList<>(lines.size());
        for (Map.Entry<String, Integer> line : lines.entrySet()) {
            String sku = line.getKey();
            int quantity = line.getValue();

            DecrementResult result = inventoryRepository.decrement(sku, quantity);
            if (result == null) {
                // Throwing rolls back the whole basket, the claim included,
                // so a corrected retry is not mistaken for a replay.
                throw CheckoutException.unknownSku(sku);
            }

            if (result.isShort(quantity)) {
                inventoryRepository.recordShortfall(transactionId, sku, quantity, result.fulfilled());
            }

            // Crossing detection without an extra query: the pre-update stock
            // is simply the new stock plus whatever we just took.
            int previousStock = result.newStock() + result.fulfilled();
            if (previousStock >= threshold && result.newStock() < threshold) {
                inventoryRepository.recordLowStockAlert(sku, result.newStock(), threshold);
            }

            outcomes.add(new StockCommitOutcome(sku, quantity, result.fulfilled()));
        }
        inventoryRepository.insertCommitLines(transactionId, outcomes);

        return new StockCommitResult(request.transactionId(), false, outcomes);
    }

    /**
     * Lines keyed by SKU, in <i>descending</i> SKU order.
     *
     * A single global lock order is the deadlock-avoidance mechanism: two
     * concurrent baskets holding {A,B} and {B,A} would otherwise take the two
     * row locks in opposite orders. Any total order works, so this one is
     * chosen for throughput. The load client's Zipf sampler makes SKU-000001
     * the hottest row by far (~12% of scans, so in most baskets), and every
     * lock is held until commit. In ascending order that row is locked first
     * and held while every other line is decremented; in descending order it
     * is locked last, so the row every basket queues on is held for as short
     * a time as possible.
     *
     * The order is inventory's correctness property, so inventory enforces it
     * rather than trusting whatever order the caller sent.
     */
    private static Map<String, Integer> mergeAndSort(List<StockCommitLine> lines) {
        if (lines == null || lines.isEmpty()) {
            throw CheckoutException.invalidRequest("A stock commit needs at least one line");
        }
        Map<String, Integer> merged = new TreeMap<>(Comparator.reverseOrder());
        for (StockCommitLine line : lines) {
            if (line == null || line.sku() == null || line.quantity() <= 0) {
                throw CheckoutException.invalidRequest("Each line needs a sku and a positive quantity");
            }
            merged.merge(line.sku(), line.quantity(), Integer::sum);
        }
        return merged;
    }

    private static UUID parseId(String transactionId) {
        try {
            return UUID.fromString(transactionId);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw CheckoutException.invalidRequest("transactionId must be a UUID");
        }
    }
}
