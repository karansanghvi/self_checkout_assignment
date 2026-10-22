package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.domain.CheckoutException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Step 1 of the saga, and how /complete treats each state it can find. */
class TransactionServiceTest {

    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private final TransactionRepository repository = mock(TransactionRepository.class);
    private final CompletionSaga saga = mock(CompletionSaga.class);
    private final TransactionService service =
            new TransactionService(repository, saga, mock(CatalogReplica.class));

    private static TransactionRow row(String status, int itemCount) {
        return new TransactionRow(ID, "station-1", status, itemCount, BigDecimal.ONE, Instant.EPOCH, null);
    }

    @Test
    void completeClaimsThenDrivesTheSaga() {
        TransactionRow claimed = row("COMPLETING", 2);
        when(repository.claimForCompletion(ID)).thenReturn(claimed);

        service.complete(ID.toString());

        verify(saga).drive(claimed);
    }

    @Test
    void completingAgainResumesAnUnfinishedSaga() {
        TransactionRow inFlight = row("COMPLETING", 2);
        when(repository.claimForCompletion(ID)).thenReturn(null);
        when(repository.findById(ID)).thenReturn(inFlight);

        service.complete(ID.toString());

        verify(saga).drive(inFlight);
    }

    @Test
    void anEmptyOpenBasketIsRefused() {
        when(repository.claimForCompletion(ID)).thenReturn(null);
        when(repository.findById(ID)).thenReturn(row("OPEN", 0));

        assertThatThrownBy(() -> service.complete(ID.toString()))
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("empty basket");
        verify(saga, never()).drive(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void aCompletedTransactionIsNotOpen() {
        when(repository.claimForCompletion(ID)).thenReturn(null);
        when(repository.findById(ID)).thenReturn(row("COMPLETED", 2));

        assertThatThrownBy(() -> service.complete(ID.toString()))
                .hasMessage("Transaction " + ID + " is COMPLETED.");
    }

    @Test
    void completingIsReportedAsOpenOnTheWire() {
        when(repository.findById(ID)).thenReturn(row("COMPLETING", 2));

        assertThat(service.get(ID.toString()).status()).isEqualTo("OPEN");
    }
}
