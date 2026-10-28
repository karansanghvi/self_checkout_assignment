package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Events.CompletedLine;
import com.school.selfcheckout.contract.Events.TransactionCompleted;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** One batch of completions: idempotent, summed per SKU, allocated in topic order, position saved with it. */
class StockCommitServiceTest {

    private static final UUID T1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID T2 = UUID.fromString("00000000-0000-0000-0000-000000000002");
    private static final UUID T3 = UUID.fromString("00000000-0000-0000-0000-000000000003");

    private final InventoryRepository repository = mock(InventoryRepository.class);
    private final StockCommitService service = new StockCommitService(repository, 50);

    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<List<CommitLine>> lines = ArgumentCaptor.forClass(List.class);

    private static TransactionCompleted tx(UUID id, CompletedLine... lines) {
        return new TransactionCompleted(id.toString(), "station-1", List.of(lines));
    }

    private static CompletedLine line(String sku, int quantity) {
        return new CompletedLine(sku, quantity);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void everyTransactionIsNewByDefault() {
        when(repository.claimCommits(any())).thenAnswer(inv -> new HashSet<>((Collection<UUID>) inv.getArgument(0)));
    }

    @Test
    void demandIsSummedPerSkuIntoOneStockUpdate() {
        when(repository.takeStock(any())).thenReturn(List.of(
                new StockChange("SKU-A", 1000, 997), new StockChange("SKU-B", 1000, 999)));

        service.applyBatch(List.of(
                tx(T1, line("SKU-A", 2)),
                tx(T2, line("SKU-A", 1), line("SKU-B", 1))), 42);

        verify(repository).takeStock(Map.of("SKU-A", 3, "SKU-B", 1));
        verify(repository).insertCommitLines(lines.capture());
        assertThat(lines.getValue()).containsExactly(
                new CommitLine(T1, "SKU-A", 2, 2),
                new CommitLine(T2, "SKU-A", 1, 1),
                new CommitLine(T2, "SKU-B", 1, 1));
        verify(repository, never()).insertShortfalls(anyList());
    }

    @Test
    void whenASkuRunsOutTheEarlierTransactionsAreServedFirstAndTheRestIsAShortfall() {
        // 3 units left, 2 + 2 + 2 asked for.
        when(repository.takeStock(any())).thenReturn(List.of(new StockChange("SKU-A", 3, 0)));

        service.applyBatch(List.of(
                tx(T1, line("SKU-A", 2)), tx(T2, line("SKU-A", 2)), tx(T3, line("SKU-A", 2))), 7);

        verify(repository).insertCommitLines(lines.capture());
        assertThat(lines.getValue()).extracting(CommitLine::fulfilled).containsExactly(2, 1, 0);
        verify(repository).insertShortfalls(lines.capture());
        assertThat(lines.getValue()).containsExactly(
                new CommitLine(T2, "SKU-A", 2, 1), new CommitLine(T3, "SKU-A", 2, 0));
    }

    @Test
    void aTransactionAlreadyAppliedIsSkippedButThePositionStillAdvances() {
        doReturn(Set.of(T2)).when(repository).claimCommits(any());
        when(repository.takeStock(any())).thenReturn(List.of(new StockChange("SKU-B", 10, 9)));

        StockCommitService.BatchResult result = service.applyBatch(List.of(
                tx(T1, line("SKU-A", 5)), tx(T2, line("SKU-B", 1))), 99);

        verify(repository).takeStock(Map.of("SKU-B", 1));
        assertThat(result.applied()).isEqualTo(1);
        assertThat(result.replayed()).isEqualTo(1);
        verify(repository).saveNextOffset(99);
    }

    @Test
    void aDuplicateWithinOneBatchIsAppliedOnce() {
        when(repository.takeStock(any())).thenReturn(List.of(new StockChange("SKU-A", 10, 9)));

        StockCommitService.BatchResult result = service.applyBatch(List.of(
                tx(T1, line("SKU-A", 1)), tx(T1, line("SKU-A", 1))), 3);

        verify(repository).takeStock(Map.of("SKU-A", 1));
        assertThat(result.replayed()).isEqualTo(1);
    }

    @Test
    void crossingTheThresholdRaisesOneAlertPerSku() {
        when(repository.takeStock(any())).thenReturn(List.of(
                new StockChange("SKU-A", 52, 48),   // crosses 50
                new StockChange("SKU-B", 49, 45),   // already below
                new StockChange("SKU-C", 900, 899)));

        service.applyBatch(List.of(tx(T1, line("SKU-A", 4), line("SKU-B", 4), line("SKU-C", 1))), 1);

        verify(repository).insertLowStockAlerts(List.of(new StockChange("SKU-A", 52, 48)), 50);
    }

    @Test
    void aBatchOfOnlySkippedRecordsJustSavesThePosition() {
        service.applyBatch(List.of(), 12);

        verify(repository, never()).takeStock(any());
        verify(repository, never()).insertLowStockAlerts(anyList(), anyInt());
        verify(repository).saveNextOffset(12);
    }
}
