package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.contract.Events.ScanRecorded;
import com.school.selfcheckout.messaging.EventCodec;
import com.school.selfcheckout.service.analytics.pipeline.PopularityPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Instant;
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

/** Drives {@link ScanConsumer#step()} against Kafka's own in-memory MockConsumer. */
class ScanConsumerTest {

    private static final String TOPIC = "scan-events";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private final MockConsumer<String, String> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final PopularityRepository repository = mock(PopularityRepository.class);
    private final PopularityPipeline pipeline = mock(PopularityPipeline.class);
    private ScanConsumer consumer;
    private long nextOffset = 0;

    /** Publishes {@code epoch:seq:sku} as the envelope checkout's relay would send. */
    private void publish(String spec) {
        String[] parts = spec.split(":", 3);
        publishRaw(EventCodec.encode(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Instant.EPOCH,
                new ScanRecorded(parts[2])));
    }

    private void publishRaw(String value) {
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, nextOffset++, null, value));
        kafka.updateEndOffsets(Map.of(TP, nextOffset));
    }

    @BeforeEach
    void setUp() {
        kafka.updateBeginningOffsets(Map.of(TP, 0L));
        kafka.updateEndOffsets(Map.of(TP, 0L));
        when(repository.loadState()).thenReturn(new AnalyticsState(5, 0));
        consumer = new ScanConsumer(kafka, repository, pipeline);
    }

    @Test
    void firstPollStartsTheStoredEpochThenFeedsInOrder() {
        consumer.step();
        verify(pipeline).reset(5);

        publish("5:1:A");
        publish("5:2:B");
        consumer.step();

        InOrder order = inOrder(pipeline);
        order.verify(pipeline).submit("A");
        order.verify(pipeline).submit("B");
        assertThat(consumer.lastSeq()).isEqualTo(2);
    }

    @Test
    void redeliveredSeqsAreDropped() {
        consumer.step();
        publish("5:1:A");
        publish("5:2:B");
        publish("5:1:A"); // the relay crashed after publishing, before saving its position
        publish("5:2:B");
        publish("5:3:C");
        consumer.step();

        verify(pipeline, org.mockito.Mockito.times(1)).submit("A");
        verify(pipeline).submit("C");
        assertThat(consumer.duplicates()).isEqualTo(2);
        assertThat(consumer.consumed()).isEqualTo(3);
    }

    @Test
    void eventsFromAnEarlierEpochAreSkipped() {
        consumer.step();
        publish("4:900:OLD");
        publish("5:1:NEW");
        consumer.step();

        verify(pipeline, never()).submit("OLD");
        verify(pipeline).submit("NEW");
        assertThat(consumer.stale()).isEqualTo(1);
    }

    @Test
    void aResetStartsTheNewEpochAtTheCurrentPosition() throws Exception {
        consumer.step();
        publish("5:1:A");
        publish("5:2:B");
        consumer.step();

        CompletableFuture<Void> reset = CompletableFuture.runAsync(() -> consumer.reset(6));
        // The catalog feed's thread only queues the reset; the consumer thread applies it.
        while (!reset.isDone()) {
            try {
                consumer.step();
            } catch (org.apache.kafka.common.errors.WakeupException ignored) {
                // reset() woke the consumer; poll again to apply it.
            }
        }
        reset.get(1, TimeUnit.SECONDS);
        verify(repository).resetTo(6, 2);

        publish("5:3:LATE"); // relay still on the old epoch when analytics reset
        publish("6:1:NEW");
        consumer.step();

        verify(pipeline, never()).submit("LATE");
        verify(pipeline).submit("NEW");
        assertThat(consumer.lastSeq()).isEqualTo(1);
    }

    @Test
    void anEpochItNeverHeardAboutIsAdopted() {
        consumer.step();
        publish("7:1:X");
        consumer.step();

        verify(repository).resetTo(7, 0);
        verify(pipeline).reset(7);
        verify(pipeline).submit("X");
    }

    @Test
    void aResetForAnEpochAlreadyAdoptedFromTheStreamIsANoOp() throws Exception {
        consumer.step();
        publish("6:1:X"); // checkout reset first; analytics adopts epoch 6 from the stream
        consumer.step();

        CompletableFuture<Void> reset = CompletableFuture.runAsync(() -> consumer.reset(6));
        while (!reset.isDone()) {
            try {
                consumer.step();
            } catch (org.apache.kafka.common.errors.WakeupException ignored) {
                // reset() woke the consumer; poll again to apply it.
            }
        }
        reset.get(1, TimeUnit.SECONDS);

        verify(repository, org.mockito.Mockito.times(1)).resetTo(org.mockito.ArgumentMatchers.eq(6L),
                org.mockito.ArgumentMatchers.anyLong());
        assertThat(consumer.lastSeq()).isEqualTo(1);
    }

    @Test
    void malformedRecordsAreIgnored() {
        consumer.step();
        publishRaw("garbage");
        publishRaw(EventCodec.encode(5, 1, Instant.EPOCH, new com.school.selfcheckout.contract.Events.ResetRequested(1, 1)));
        consumer.step();

        verify(pipeline, never()).submit(anyString());
    }
}
