package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.Receipt;
import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.domain.ErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Steps 2 and 3 of the completion saga, and what happens when step 2 fails. */
class CompletionSagaTest {

    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final List<TransactionLine> LINES = List.of(
            new TransactionLine("SKU-000001", 2, new BigDecimal("0.85")),
            new TransactionLine("SKU-000002", 1, new BigDecimal("1.20")));

    private final TransactionRepository repository = mock(TransactionRepository.class);
    private final InventoryClient inventory = mock(InventoryClient.class);
    private final CatalogReplica catalog = mock(CatalogReplica.class);
    private final CompletionSaga saga = new CompletionSaga(repository, inventory, catalog);

    private static TransactionRow row(String status, Instant completedAt) {
        return new TransactionRow(ID, "station-1", status, 3, new BigDecimal("2.90"), Instant.EPOCH, completedAt);
    }

    @BeforeEach
    void setUp() {
        when(repository.findLinesOrderedBySku(ID)).thenReturn(LINES);
        when(catalog.findBySku(anyString())).thenAnswer(inv ->
                new CatalogItem(inv.getArgument(0), "Item", BigDecimal.ONE));
    }

    @Test
    void commitsStockBeforeMarkingTheTransactionCompleted() {
        when(repository.markCompleted(ID)).thenReturn(row("COMPLETED", Instant.EPOCH.plusSeconds(5)));

        Receipt receipt = saga.drive(row("COMPLETING", null));

        InOrder order = inOrder(inventory, repository);
        order.verify(inventory).commit(ID, LINES);
        order.verify(repository).markCompleted(ID);
        assertThat(receipt.lines()).hasSize(2);
        assertThat(receipt.totalAmount()).isEqualByComparingTo("2.90");
        assertThat(receipt.completedAt()).isEqualTo(Instant.EPOCH.plusSeconds(5));
    }

    @Test
    void inventoryUnavailableLeavesTheSagaInFlightForRetry() {
        when(inventory.commit(any(), any())).thenThrow(new InventoryClient.Unavailable("Connection refused"));

        assertThatThrownBy(() -> saga.drive(row("COMPLETING", null)))
                .isInstanceOf(CheckoutException.class)
                .extracting(e -> ((CheckoutException) e).getErrorCode())
                .isEqualTo(ErrorCode.DEPENDENCY_FAILED);

        // Neither completed (stock may not be taken) nor reverted (it may have been).
        verify(repository, never()).markCompleted(any());
        verify(repository, never()).revertToOpen(any());
    }

    @Test
    void inventoryRejectionIsCompensatedByReopening() {
        when(inventory.commit(any(), any())).thenThrow(new InventoryClient.Rejected("404 UNKNOWN_SKU"));

        assertThatThrownBy(() -> saga.drive(row("COMPLETING", null)))
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("OPEN again");

        verify(repository).revertToOpen(ID);
        verify(repository, never()).markCompleted(any());
    }

    @Test
    void aRaceLostToAnotherAttemptStillReturnsTheReceipt() {
        when(repository.markCompleted(ID)).thenReturn(null);
        when(repository.findById(ID)).thenReturn(row("COMPLETED", Instant.EPOCH.plusSeconds(9)));

        Receipt receipt = saga.drive(row("COMPLETING", null));

        assertThat(receipt.completedAt()).isEqualTo(Instant.EPOCH.plusSeconds(9));
    }
}
