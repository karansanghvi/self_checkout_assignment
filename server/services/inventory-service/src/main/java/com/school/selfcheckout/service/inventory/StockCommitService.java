package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Events.CompletedLine;
import com.school.selfcheckout.contract.Events.TransactionCompleted;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The stock-mutating half of inventory: applies completed transactions,
 * consumed from {@code transaction-completed}, a batch at a time.
 *
 * In week 5 this was {@code POST /internal/stock-commits}, step 2 of a
 * synchronous saga that checkout-service drove for every basket. Now
 * checkout has already answered the client, and this applies the
 * transaction afterwards. Each batch is one inventory_db transaction:
 *
 * <ol>
 *   <li><b>Idempotency.</b> Each transaction id is claimed in
 *       {@code stock_commits}; one already there is a redelivery and is
 *       skipped.</li>
 *   <li><b>One update per SKU.</b> The claimed transactions' demand is
 *       summed per SKU and taken out of stock in a single statement,
 *       clamped at zero (see {@link InventoryRepository#takeStock}). With
 *       ~10 lines per basket and the Zipf-hot SKUs in most of them, this
 *       turns thousands of updates into a few hundred, and the hottest row
 *       is updated once per batch instead of once per basket.</li>
 *   <li><b>Allocation.</b> The units actually taken are handed out to the
 *       transactions in topic order, first come first served. Where a SKU
 *       ran out partway through a batch, the later transactions get less,
 *       and the shortfall is recorded -- the contract gives {@code /complete}
 *       no out-of-stock error, so the sale stands either way.</li>
 *   <li><b>Position.</b> The consumer's next topic offset is saved in the
 *       same transaction, so the stock and the position can never
 *       disagree.</li>
 * </ol>
 */
@Service
public class StockCommitService {

    /** What one batch did. */
    public record BatchResult(int applied, int replayed) {
    }

    private final InventoryRepository inventoryRepository;
    private final int threshold;

    public StockCommitService(InventoryRepository inventoryRepository,
                              @Value("${app.low-stock-threshold}") int threshold) {
        this.inventoryRepository = inventoryRepository;
        this.threshold = threshold;
    }

    /**
     * @param completions valid, known-SKU transactions, in topic order
     * @param nextOffset  the topic offset to resume from once this batch is committed
     */
    @Transactional
    public BatchResult applyBatch(List<TransactionCompleted> completions, long nextOffset) {
        Map<UUID, Map<String, Integer>> byTransaction = new LinkedHashMap<>();
        for (TransactionCompleted completion : completions) {
            byTransaction.putIfAbsent(UUID.fromString(completion.transactionId()), merge(completion.lines()));
        }

        Set<UUID> claimed = byTransaction.isEmpty() ? Set.of() : inventoryRepository.claimCommits(byTransaction.keySet());
        byTransaction.keySet().retainAll(claimed);

        Map<String, Integer> demand = new TreeMap<>();
        byTransaction.values().forEach(lines -> lines.forEach((sku, qty) -> demand.merge(sku, qty, Integer::sum)));

        if (!demand.isEmpty()) {
            List<StockChange> changes = inventoryRepository.takeStock(demand);
            Map<String, Integer> remaining = new HashMap<>();
            List<StockChange> crossings = new ArrayList<>();
            for (StockChange change : changes) {
                remaining.put(change.sku(), change.before() - change.after());
                if (change.before() >= threshold && change.after() < threshold) {
                    crossings.add(change);
                }
            }

            List<CommitLine> lines = new ArrayList<>();
            List<CommitLine> shortfalls = new ArrayList<>();
            byTransaction.forEach((transactionId, txLines) -> txLines.forEach((sku, requested) -> {
                int available = remaining.getOrDefault(sku, 0);
                int fulfilled = Math.min(requested, available);
                remaining.put(sku, available - fulfilled);
                CommitLine line = new CommitLine(transactionId, sku, requested, fulfilled);
                lines.add(line);
                if (line.isShort()) {
                    shortfalls.add(line);
                }
            }));

            inventoryRepository.insertCommitLines(lines);
            if (!shortfalls.isEmpty()) {
                inventoryRepository.insertShortfalls(shortfalls);
            }
            if (!crossings.isEmpty()) {
                inventoryRepository.insertLowStockAlerts(crossings, threshold);
            }
        }

        inventoryRepository.saveNextOffset(nextOffset);
        return new BatchResult(byTransaction.size(), completions.size() - byTransaction.size());
    }

    /** One quantity per SKU, in SKU order. */
    private static Map<String, Integer> merge(List<CompletedLine> lines) {
        Map<String, Integer> merged = new TreeMap<>();
        for (CompletedLine line : lines) {
            merged.merge(line.sku(), line.quantity(), Integer::sum);
        }
        return merged;
    }
}
