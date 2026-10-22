package com.school.selfcheckout.service.checkout;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Drives {@link OutboxRelay#publishOnce()} against a scripted outbox and a recording publisher. */
class OutboxRelayTest {

    private final OutboxRepository outbox = mock(OutboxRepository.class);
    private final List<String> published = new ArrayList<>();
    private RuntimeException publishFailure;

    private final ScanPublisher publisher = (epoch, events) -> {
        if (publishFailure != null) {
            throw publishFailure;
        }
        events.forEach(e -> published.add(epoch + ":" + e.seq() + ":" + e.sku()));
    };

    private static OutboxEvent ev(long seq, String sku) {
        return new OutboxEvent(seq, sku);
    }

    @BeforeEach
    void setUp() {
        when(outbox.loadState()).thenReturn(new RelayState(7, 0));
    }

    @Test
    void publishesInSeqOrderTaggedWithTheEpochThenSavesThePosition() {
        OutboxRelay relay = new OutboxRelay(outbox, publisher);
        when(outbox.poll(eq(0L), anyInt())).thenReturn(List.of(ev(1, "A"), ev(2, "B")));

        relay.publishOnce();

        assertThat(published).containsExactly("7:1:A", "7:2:B");
        verify(outbox).savePublishedSeq(2);
        assertThat(relay.lastSeq()).isEqualTo(2);
    }

    @Test
    void resumesFromTheSavedPositionAfterARestart() {
        when(outbox.loadState()).thenReturn(new RelayState(7, 41));
        OutboxRelay relay = new OutboxRelay(outbox, publisher);
        when(outbox.poll(eq(41L), anyInt())).thenReturn(List.of(ev(42, "A")));

        relay.publishOnce();

        assertThat(published).containsExactly("7:42:A");
    }

    @Test
    void stopsAtAGapUntilTheMissingSeqCommits() {
        OutboxRelay relay = new OutboxRelay(outbox, publisher);
        when(outbox.poll(eq(1L), anyInt())).thenReturn(List.of(ev(2, "B"), ev(3, "C")));
        when(outbox.poll(eq(0L), anyInt())).thenReturn(List.of(ev(1, "A"), ev(3, "C")));

        relay.publishOnce(); // seq 2 not yet visible: publish 1, hold back 3
        assertThat(published).containsExactly("7:1:A");

        relay.publishOnce(); // seq 2 committed late
        assertThat(published).containsExactly("7:1:A", "7:2:B", "7:3:C");
        assertThat(relay.skippedSeqs()).isZero();
    }

    @Test
    void skipsASeqThatNeverCommits() throws Exception {
        OutboxRelay relay = new OutboxRelay(outbox, publisher, TimeUnit.MILLISECONDS.toNanos(1));
        when(outbox.poll(eq(0L), anyInt())).thenReturn(List.of(ev(2, "B")));

        relay.publishOnce(); // gap at seq 1 first seen
        Thread.sleep(5);
        relay.publishOnce(); // gap outlived its timeout: give up on seq 1

        assertThat(published).containsExactly("7:2:B");
        assertThat(relay.skippedSeqs()).isEqualTo(1);
    }

    @Test
    void aFailedPublishDoesNotAdvanceThePosition() {
        OutboxRelay relay = new OutboxRelay(outbox, publisher);
        when(outbox.poll(eq(0L), anyInt())).thenReturn(List.of(ev(1, "A")));
        publishFailure = new IllegalStateException("Kafka down");

        assertThatThrownBy(relay::publishOnce).isInstanceOf(IllegalStateException.class);

        verify(outbox, never()).savePublishedSeq(anyLong());
        assertThat(relay.lastSeq()).isZero();

        publishFailure = null;
        relay.publishOnce(); // The whole batch goes again.
        assertThat(published).containsExactly("7:1:A");
    }

    @Test
    void aResetRewindsTheRelayAndRetagsUnderTheNewEpoch() {
        OutboxRelay relay = new OutboxRelay(outbox, publisher);
        when(outbox.poll(eq(0L), anyInt()))
                .thenReturn(List.of(ev(1, "OLD")))
                .thenReturn(List.of(ev(1, "NEW")));

        relay.publishOnce();
        relay.reset(8);
        relay.publishOnce();

        InOrder order = inOrder(outbox);
        order.verify(outbox).savePublishedSeq(1);
        order.verify(outbox).resetTo(8);
        order.verify(outbox).savePublishedSeq(1);
        assertThat(published).containsExactly("7:1:OLD", "8:1:NEW");
    }
}
