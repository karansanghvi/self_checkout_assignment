package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.InternalDtos.StockCommitLine;
import com.school.selfcheckout.contract.InternalDtos.StockCommitOutcome;
import com.school.selfcheckout.contract.InternalDtos.StockCommitRequest;
import com.school.selfcheckout.contract.InternalDtos.StockCommitResult;
import com.school.selfcheckout.domain.CheckoutException;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** The pivot step of the completion saga: idempotent, ordered, all-or-nothing. */
class StockCommitServiceTest {

    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000003");

    private final InventoryRepository repository = mock(InventoryRepository.class);
    private final StockCommitService service = new StockCommitService(repository, 50);

    private static StockCommitRequest request(StockCommitLine... lines) {
        return new StockCommitRequest(ID.toString(), List.of(lines));
    }

    @Test
    void decrementsInDescendingSkuOrderWhateverOrderTheLinesArriveIn() {
        when(repository.claimCommit(ID)).thenReturn(true);
        when(repository.decrement(anyString(), anyInt())).thenReturn(new DecrementResult(1, 500));

        service.commit(request(new StockCommitLine("SKU-000001", 1),
                new StockCommitLine("SKU-000003", 1), new StockCommitLine("SKU-000002", 1)));

        // The hottest row, SKU-000001, is locked last.
        InOrder order = inOrder(repository);
        order.verify(repository).decrement("SKU-000003", 1);
        order.verify(repository).decrement("SKU-000002", 1);
        order.verify(repository).decrement("SKU-000001", 1);
    }

    @Test
    void aRepeatedCommitReturnsTheOriginalOutcomeAndTouchesNoStock() {
        List<StockCommitOutcome> original = List.of(new StockCommitOutcome("SKU-A", 2, 2));
        when(repository.claimCommit(ID)).thenReturn(false);
        when(repository.findCommitLines(ID)).thenReturn(original);

        StockCommitResult result = service.commit(request(new StockCommitLine("SKU-A", 2)));

        assertThat(result.replayed()).isTrue();
        assertThat(result.lines()).isEqualTo(original);
        verify(repository, never()).decrement(anyString(), anyInt());
    }

    @Test
    void aShortLineIsClampedAndRecordedNotRefused() {
        when(repository.claimCommit(ID)).thenReturn(true);
        when(repository.decrement("SKU-A", 5)).thenReturn(new DecrementResult(3, 0));

        StockCommitResult result = service.commit(request(new StockCommitLine("SKU-A", 5)));

        assertThat(result.lines()).containsExactly(new StockCommitOutcome("SKU-A", 5, 3));
        verify(repository).recordShortfall(ID, "SKU-A", 5, 3);
        verify(repository).insertCommitLines(eq(ID), eq(result.lines()));
    }

    @Test
    void recordsALowStockAlertOnlyOnTheCrossing() {
        when(repository.claimCommit(ID)).thenReturn(true);
        when(repository.decrement("SKU-A", 2)).thenReturn(new DecrementResult(2, 49));
        when(repository.decrement("SKU-B", 2)).thenReturn(new DecrementResult(2, 40));

        service.commit(request(new StockCommitLine("SKU-A", 2), new StockCommitLine("SKU-B", 2)));

        verify(repository).recordLowStockAlert("SKU-A", 49, 50);
        verify(repository, never()).recordLowStockAlert(eq("SKU-B"), anyInt(), anyInt());
    }

    @Test
    void anUnknownSkuFailsTheWholeBasket() {
        when(repository.claimCommit(ID)).thenReturn(true);
        when(repository.decrement("SKU-A", 1)).thenReturn(new DecrementResult(1, 9));
        when(repository.decrement("SKU-Z", 1)).thenReturn(null);

        assertThatThrownBy(() -> service.commit(
                request(new StockCommitLine("SKU-A", 1), new StockCommitLine("SKU-Z", 1))))
                .isInstanceOf(CheckoutException.class)
                .hasMessage("No such SKU: SKU-Z");
        // The @Transactional boundary rolls back SKU-A's decrement and the claim.
        verify(repository, never()).insertCommitLines(any(), any());
    }

    @Test
    void duplicateLinesForOneSkuAreMerged() {
        when(repository.claimCommit(ID)).thenReturn(true);
        when(repository.decrement("SKU-A", 3)).thenReturn(new DecrementResult(3, 100));

        service.commit(request(new StockCommitLine("SKU-A", 1), new StockCommitLine("SKU-A", 2)));

        verify(repository).decrement("SKU-A", 3);
    }

    @Test
    void anEmptyCommitIsInvalid() {
        assertThatThrownBy(() -> service.commit(request()))
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("at least one line");
        verify(repository, never()).claimCommit(any());
    }
}
