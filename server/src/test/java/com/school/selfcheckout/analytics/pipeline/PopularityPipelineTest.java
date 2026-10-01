package com.school.selfcheckout.analytics.pipeline;

import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.domain.PopularEntry;
import com.school.selfcheckout.repository.PopularityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Drives the real three-filter pipeline, threads and all, against a mocked repository. */
class PopularityPipelineTest {

    private final PopularityRepository repository = mock(PopularityRepository.class);
    private PopularityPipeline pipeline;

    private PopularityPipeline newPipeline(int windowSize, int slideInterval) {
        AppProperties properties = new AppProperties();
        properties.getPopularity().setWindowSize(windowSize);
        properties.getPopularity().setSlideInterval(slideInterval);
        pipeline = new PopularityPipeline(repository, properties);
        pipeline.start();
        return pipeline;
    }

    @AfterEach
    void stopPipeline() {
        if (pipeline != null) {
            pipeline.stop();
        }
    }

    @Test
    void emitsOneSnapshotPerHopWithContractWindowBounds() {
        newPipeline(1000, 500);

        for (int i = 0; i < 1500; i++) {
            pipeline.submit("SKU-" + (i % 7));
        }

        verify(repository, timeout(2000)).saveWindow(eq(0L), eq(500L), eq(1000), eq(500), anyList());
        verify(repository, timeout(2000)).saveWindow(eq(0L), eq(1000L), eq(1000), eq(500), anyList());
        verify(repository, timeout(2000)).saveWindow(eq(500L), eq(1500L), eq(1000), eq(500), anyList());
        assertThat(pipeline.currentSequence()).isEqualTo(1500);
        assertThat(pipeline.droppedMessages()).isZero();
    }

    @Test
    @SuppressWarnings("unchecked")
    void ranksByCountThenSkuAndCountsOnlyTheWindow() {
        newPipeline(4, 4);

        // Window of 4: B twice, A and C once each. A beats C on the SKU tie-break.
        for (String sku : List.of("C", "B", "A", "B")) {
            pipeline.submit(sku);
        }

        ArgumentCaptor<List<PopularEntry>> entries = ArgumentCaptor.forClass(List.class);
        verify(repository, timeout(2000)).saveWindow(eq(0L), eq(4L), eq(4), eq(4), entries.capture());
        assertThat(entries.getValue()).containsExactly(
                new PopularEntry(1, "B", 2),
                new PopularEntry(2, "A", 1),
                new PopularEntry(3, "C", 1));
    }

    @Test
    void resetClearsTheWindowBeforeLaterScansArrive() {
        newPipeline(10, 10);

        for (int i = 0; i < 7; i++) {
            pipeline.submit("OLD");
        }
        pipeline.reset();
        assertThat(pipeline.currentSequence()).isZero();

        for (int i = 0; i < 10; i++) {
            pipeline.submit("NEW");
        }

        // Without the reset this hop would have closed at scan 10 with 7 OLDs in it.
        verify(repository, timeout(2000)).saveWindow(
                eq(0L), eq(10L), eq(10), eq(10), eq(List.of(new PopularEntry(1, "NEW", 10))));
        verify(repository, times(1)).saveWindow(anyLong(), anyLong(), anyInt(), anyInt(), anyList());
    }

    @Test
    void persistFailureDoesNotStopThePipeline() throws Exception {
        newPipeline(2, 2);
        org.mockito.Mockito.doThrow(new RuntimeException("db down")).doNothing()
                .when(repository).saveWindow(anyLong(), anyLong(), anyInt(), anyInt(), anyList());

        for (int i = 0; i < 4; i++) {
            pipeline.submit("X");
        }

        verify(repository, timeout(2000)).saveWindow(eq(2L), eq(4L), eq(2), eq(2), anyList());
    }

    @Test
    void nothingIsPersistedBeforeTheFirstHop() throws Exception {
        newPipeline(1000, 500);

        for (int i = 0; i < 499; i++) {
            pipeline.submit("X");
        }
        pipeline.reset(); // Doubles as a barrier: returns once everything ahead of it was processed.

        verify(repository, never()).saveWindow(anyLong(), anyLong(), anyInt(), anyInt(), anyList());
    }

    @Test
    void scanPipeDropsNewestWhenFull() {
        Pipe<String> pipe = new Pipe<>("test", 2, Pipe.Overflow.DROP_NEWEST);
        pipe.send("a");
        pipe.send("b");
        pipe.send("c");

        assertThat(pipe.dropped()).isEqualTo(1);
    }

    @Test
    void snapshotPipeDropsOldestDataButNeverControlMessages() throws Exception {
        Pipe<String> pipe = new Pipe<>("test", 2, Pipe.Overflow.DROP_OLDEST);
        assertThat(pipe.sendControl(new Message.Stop<>(), 1, TimeUnit.SECONDS)).isTrue();
        pipe.send("old");
        pipe.send("new");

        assertThat(pipe.dropped()).isEqualTo(1);
        assertThat(pipe.take()).isInstanceOf(Message.Stop.class);
        assertThat(pipe.take()).isEqualTo(new Message.Data<>("new"));
    }
}
