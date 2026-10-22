package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.service.analytics.pipeline.PopularityPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Drives {@link ScanConsumer#pollOnce()} against Kafka's own in-memory MockConsumer. */
class ScanConsumerTest {

    private static final String TOPIC = "scan-events";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private final MockConsumer<String, String> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final PopularityRepository repository = mock(PopularityRepository.class);
    private final PopularityPipeline pipeline = mock(PopularityPipeline.class);
    private ScanConsumer consumer;
    private long nextOffset = 0;

    private void publish(String value) {
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, nextOffset++, null, value));
        kafka.updateEndOffsets(Map.of(TP, nextOffset));
    }

    @BeforeEach
    void setUp() {
        kafka.updateBeginningOffsets(Map.of(TP, 0L));
        kafka.updateEndOffsets(Map.of(TP, 0L));
        when(repository.loadState()).thenReturn(new AnalyticsState(5, 0));
        consumer = new ScanConsumer(kafka, repository, pipeline, TOPIC);
    }

    @Test
    void firstPollStartsTheStoredEpochThenFeedsInOrder() {
        consumer.pollOnce();
        verify(pipeline).reset(5);

        publish("5:1:A");
        publish("5:2:B");
        consumer.pollOnce();

        InOrder order = inOrder(pipeline);
        order.verify(pipeline).submit("A");
        order.verify(pipeline).submit("B");
        assertThat(consumer.lastSeq()).isEqualTo(2);
    }

    @Test
    void redeliveredSeqsAreDropped() {
        consumer.pollOnce();
        publish("5:1:A");
        publish("5:2:B");
        publish("5:1:A"); // the relay crashed after publishing, before saving its position
        publish("5:2:B");
        publish("5:3:C");
        consumer.pollOnce();

        verify(pipeline, org.mockito.Mockito.times(1)).submit("A");
        verify(pipeline).submit("C");
        assertThat(consumer.duplicates()).isEqualTo(2);
        assertThat(consumer.consumed()).isEqualTo(3);
    }

    @Test
    void eventsFromAnEarlierEpochAreSkipped() {
        consumer.pollOnce();
        publish("4:900:OLD");
        publish("5:1:NEW");
        consumer.pollOnce();

        verify(pipeline, never()).submit("OLD");
        verify(pipeline).submit("NEW");
        assertThat(consumer.stale()).isEqualTo(1);
    }

    @Test
    void aResetStartsTheNewEpochAtTheEndOfTheTopic() throws Exception {
        consumer.pollOnce();
        publish("5:1:A");
        publish("5:2:B");
        consumer.pollOnce();

        CompletableFuture<Void> reset = CompletableFuture.runAsync(() -> consumer.reset(6));
        // The HTTP thread only queues the reset; the consumer thread applies it.
        while (!reset.isDone()) {
            try {
                consumer.pollOnce();
            } catch (org.apache.kafka.common.errors.WakeupException ignored) {
                // reset() woke the consumer; poll again to apply it.
            }
        }
        reset.get(1, TimeUnit.SECONDS);
        verify(repository).resetTo(6, 2);

        publish("5:3:LATE"); // relay still on the old epoch when analytics reset
        publish("6:1:NEW");
        consumer.pollOnce();

        verify(pipeline, never()).submit("LATE");
        verify(pipeline).submit("NEW");
        assertThat(consumer.lastSeq()).isEqualTo(1);
    }

    @Test
    void anEpochItNeverHeardAboutIsAdopted() {
        consumer.pollOnce();
        publish("7:1:X");
        consumer.pollOnce();

        verify(repository).resetTo(7, 0);
        verify(pipeline).reset(7);
        verify(pipeline).submit("X");
    }

    @Test
    void malformedRecordsAreIgnored() {
        consumer.pollOnce();
        publish("garbage");
        consumer.pollOnce();

        verify(pipeline, never()).submit(anyString());
    }
}
