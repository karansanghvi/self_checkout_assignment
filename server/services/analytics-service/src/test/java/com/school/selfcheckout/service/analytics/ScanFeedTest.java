package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.data.analytics.ScanBatch;
import com.school.selfcheckout.data.analytics.ScanEvent;
import com.school.selfcheckout.data.analytics.ScanEventRepository;
import com.school.selfcheckout.service.analytics.pipeline.PopularityPipeline;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Drives {@link ScanFeed#pollOnce()} directly against a scripted scan_events table. */
class ScanFeedTest {

    private final ScanEventRepository repository = mock(ScanEventRepository.class);
    private final PopularityPipeline pipeline = mock(PopularityPipeline.class);

    private static ScanBatch batch(long epoch, ScanEvent... events) {
        return new ScanBatch(epoch, List.of(events));
    }

    private static ScanEvent ev(long seq, String sku) {
        return new ScanEvent(seq, sku);
    }

    @Test
    void firstPollResetsThePipelineThenFeedsInSeqOrder() {
        ScanFeed feed = new ScanFeed(repository, pipeline);
        when(repository.poll(eq(0L), anyInt()))
                .thenReturn(batch(3, ev(1, "A")))
                .thenReturn(batch(3, ev(1, "A"), ev(2, "B")));

        assertThat(feed.pollOnce()).as("a reset asks to re-poll at once").isTrue();
        verify(pipeline).reset(3);
        verify(pipeline, never()).submit(anyString());

        feed.pollOnce();
        InOrder order = inOrder(pipeline);
        order.verify(pipeline).submit("A");
        order.verify(pipeline).submit("B");
        assertThat(feed.lastSeq()).isEqualTo(2);
    }

    @Test
    void stopsAtAGapUntilTheMissingSeqCommits() {
        ScanFeed feed = new ScanFeed(repository, pipeline);
        // Stub the poll(0) chain last: stubbing calls the mock, and its matchers
        // evaluate to 0, so a later when(...) would eat one of its answers.
        when(repository.poll(eq(1L), anyInt()))
                .thenReturn(batch(1, ev(2, "B"), ev(3, "C")));
        when(repository.poll(eq(0L), anyInt()))
                .thenReturn(batch(1))
                .thenReturn(batch(1, ev(1, "A"), ev(3, "C")));

        feed.pollOnce(); // epoch baseline
        feed.pollOnce(); // seq 2 not yet visible: consume 1, hold back 3
        assertThat(feed.lastSeq()).isEqualTo(1);
        verify(pipeline, never()).submit("C");

        feed.pollOnce(); // seq 2 committed late
        InOrder order = inOrder(pipeline);
        order.verify(pipeline).submit("A");
        order.verify(pipeline).submit("B");
        order.verify(pipeline).submit("C");
        assertThat(feed.skippedSeqs()).isZero();
    }

    @Test
    void skipsASeqThatNeverCommits() throws Exception {
        ScanFeed feed = new ScanFeed(repository, pipeline, TimeUnit.MILLISECONDS.toNanos(1));
        when(repository.poll(anyLong(), anyInt())).thenReturn(batch(1));
        when(repository.poll(eq(0L), anyInt()))
                .thenReturn(batch(1))
                .thenReturn(batch(1, ev(2, "B")));

        feed.pollOnce();       // epoch baseline
        feed.pollOnce();       // gap at seq 1 first seen
        Thread.sleep(5);
        feed.pollOnce();       // gap outlived its timeout: give up on seq 1

        verify(pipeline).submit("B");
        assertThat(feed.lastSeq()).isEqualTo(2);
        assertThat(feed.skippedSeqs()).isEqualTo(1);
    }

    @Test
    void anEpochChangeResetsThePipelineAndRewindsTheFeed() {
        ScanFeed feed = new ScanFeed(repository, pipeline);
        when(repository.poll(eq(2L), anyInt())).thenReturn(batch(2));
        when(repository.poll(eq(0L), anyInt()))
                .thenReturn(batch(1))
                .thenReturn(batch(1, ev(1, "OLD"), ev(2, "OLD")))
                .thenReturn(batch(2, ev(1, "NEW")));

        feed.pollOnce();
        feed.pollOnce();
        assertThat(feed.lastSeq()).isEqualTo(2);

        assertThat(feed.pollOnce()).isTrue(); // admin-service reset: epoch 1 -> 2
        assertThat(feed.lastSeq()).isZero();
        feed.pollOnce();

        InOrder order = inOrder(pipeline);
        order.verify(pipeline).reset(1);
        order.verify(pipeline, org.mockito.Mockito.times(2)).submit("OLD");
        order.verify(pipeline).reset(2);
        order.verify(pipeline).submit("NEW");
    }
}
