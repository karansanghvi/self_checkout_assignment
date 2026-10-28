package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.Receipt;
import com.school.selfcheckout.domain.CheckoutException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** /complete is one local transaction now; these pin the receipt and how each state it can find is treated. */
class TransactionServiceTest {

    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private final TransactionRepository repository = mock(TransactionRepository.class);
    private final CatalogReplica catalog = new CatalogReplica();
    private final TransactionService service = new TransactionService(repository, catalog);

    private static TransactionRow row(String status, int itemCount) {
        return new TransactionRow(ID, "station-1", status, itemCount, new BigDecimal("2.70"), Instant.EPOCH,
                "COMPLETED".equals(status) ? Instant.EPOCH.plusSeconds(5) : null);
    }

    @Test
    void completeReturnsAReceiptBuiltFromTheCompletedLines() {
        catalog.apply(List.of(new CatalogItem("SKU-A", "Item A", new BigDecimal("0.85"))));
        when(repository.complete(ID)).thenReturn(new Completion(row("COMPLETED", 3), List.of(
                new TransactionLine("SKU-A", 2, new BigDecimal("0.85")),
                new TransactionLine("SKU-B", 1, new BigDecimal("1.00")))));

        Receipt receipt = service.complete(ID.toString());

        assertThat(receipt.itemCount()).isEqualTo(3);
        assertThat(receipt.totalAmount()).isEqualByComparingTo("2.70");
        assertThat(receipt.completedAt()).isEqualTo(Instant.EPOCH.plusSeconds(5));
        assertThat(receipt.lines()).extracting("sku", "name", "quantity")
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("SKU-A", "Item A", 2),
                        org.assertj.core.groups.Tuple.tuple("SKU-B", "SKU-B", 1)); // not in the replica: falls back to the SKU
    }

    @Test
    void anEmptyOpenBasketIsRefused() {
        when(repository.findById(ID)).thenReturn(row("OPEN", 0));

        assertThatThrownBy(() -> service.complete(ID.toString()))
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("empty basket");
    }

    @Test
    void aCompletedTransactionIsNotOpen() {
        when(repository.findById(ID)).thenReturn(row("COMPLETED", 2));

        assertThatThrownBy(() -> service.complete(ID.toString()))
                .hasMessage("Transaction " + ID + " is COMPLETED.");
    }

    @Test
    void aMissingTransactionIsNotFound() {
        assertThatThrownBy(() -> service.complete(ID.toString()))
                .hasMessage("No such transaction: " + ID);
    }

    @Test
    void aBasketWhoseFirstScanLandedMidCompletionIsClaimedAgain() {
        when(repository.findById(ID)).thenReturn(row("OPEN", 1));
        when(repository.complete(ID))
                .thenReturn(null)
                .thenReturn(new Completion(row("COMPLETED", 1), List.of(
                        new TransactionLine("SKU-A", 1, new BigDecimal("0.85")))));

        assertThat(service.complete(ID.toString()).itemCount()).isEqualTo(1);
        verify(repository, times(2)).complete(ID);
    }

    @Test
    void aScanBeforeTheFirstCatalogSnapshotIsADependencyFailure() {
        assertThatThrownBy(() -> service.scan(ID.toString(), "SKU-A"))
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("catalog snapshot");
    }
}
