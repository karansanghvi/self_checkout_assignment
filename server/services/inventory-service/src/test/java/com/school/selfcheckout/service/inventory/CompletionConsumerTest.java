package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Events.CatalogSnapshot;
import com.school.selfcheckout.contract.Events.CompletedLine;
import com.school.selfcheckout.contract.Events.TransactionCompleted;
import com.school.selfcheckout.messaging.EventCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Drives {@link CompletionConsumer#step()} against Kafka's in-memory MockConsumer. */
class CompletionConsumerTest {

    private static final String TOPIC = "transaction-completed";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    private static final String ID_1 = "00000000-0000-0000-0000-000000000001";
    private static final String ID_2 = "00000000-0000-0000-0000-000000000002";

    private final MockConsumer<String, String> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final InventoryRepository repository = mock(InventoryRepository.class);
    private final StockCommitService stockCommits = mock(StockCommitService.class);
    private final DeadLetters deadLetters = mock(DeadLetters.class);
    private CompletionConsumer consumer;
    private long nextOffset = 10;

    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<List<TransactionCompleted>> batch = ArgumentCaptor.forClass(List.class);

    private void publish(String value) {
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, nextOffset++, null, value));
    }

    private static String completion(long epoch, String id, String sku) {
        return EventCodec.encode(epoch, 1, Instant.now(),
                new TransactionCompleted(id, "station-1", List.of(new CompletedLine(sku, 1))));
    }

    @BeforeEach
    void setUp() {
        kafka.updateBeginningOffsets(Map.of(TP, 0L));
        when(repository.loadState()).thenReturn(new InventoryState(5, 10));
        when(repository.findAllSkus()).thenReturn(Set.of("SKU-A", "SKU-B"));
        when(stockCommits.applyBatch(anyList(), anyLong())).thenReturn(new StockCommitService.BatchResult(0, 0));
        consumer = new CompletionConsumer(kafka, repository, stockCommits, deadLetters);
    }

    @Test
    void resumesFromTheOffsetSavedInInventoryDb() {
        consumer.step();

        assertThat(kafka.position(TP)).isEqualTo(10);
    }

    @Test
    void appliesOnePollAsOneBatchAndSavesThePositionAfterIt() {
        consumer.step();
        publish(completion(5, ID_1, "SKU-A"));
        publish(completion(5, ID_2, "SKU-B"));
        consumer.step();

        verify(stockCommits).applyBatch(batch.capture(), eq(12L));
        assertThat(batch.getValue()).extracting(TransactionCompleted::transactionId).containsExactly(ID_1, ID_2);
    }

    @Test
    void completionsFromAnEarlierEpochAreDroppedButStillAdvanceThePosition() {
        consumer.step();
        publish(completion(4, ID_1, "SKU-A"));
        consumer.step();

        verify(stockCommits).applyBatch(List.of(), 11L);
        assertThat(consumer.stale()).isEqualTo(1);
    }

    @Test
    void unprocessableRecordsAreDeadLetteredAndDoNotBlockTheRest() {
        consumer.step();
        publish("garbage");
        publish(completion(5, ID_1, "SKU-UNKNOWN"));
        publish(completion(5, ID_2, "SKU-A"));
        consumer.step();

        verify(deadLetters, org.mockito.Mockito.times(2)).send(any(), any());
        verify(stockCommits).applyBatch(batch.capture(), eq(13L));
        assertThat(batch.getValue()).extracting(TransactionCompleted::transactionId).containsExactly(ID_2);
    }

    @Test
    void aCompletionFromANewerEpochPausesThePartitionUntilTheResetArrives() throws Exception {
        consumer.step();
        publish(completion(5, ID_1, "SKU-A"));
        publish(completion(6, ID_2, "SKU-B"));
        consumer.step();

        verify(stockCommits).applyBatch(batch.capture(), eq(11L));
        assertThat(batch.getValue()).hasSize(1);
        assertThat(consumer.isPaused()).isTrue();
        assertThat(kafka.position(TP)).isEqualTo(11); // rewound to the epoch-6 record

        CatalogSnapshot snapshot = new CatalogSnapshot(100, List.of(
                new CatalogItem("SKU-A", "Item A", BigDecimal.ONE), new CatalogItem("SKU-B", "Item B", BigDecimal.ONE)));
        CompletableFuture<Void> reset = CompletableFuture.runAsync(() -> consumer.resetTo(6, snapshot));
        while (!reset.isDone()) {
            try {
                consumer.step();
            } catch (WakeupException ignored) {
                // resetTo woke the consumer; step again to run the reset.
            }
        }
        reset.get(1, TimeUnit.SECONDS);

        verify(repository).reseed(6, snapshot.items(), 100);
        assertThat(consumer.isPaused()).isFalse();
    }

    @Test
    void aFailedBatchRewindsSoTheWholeBatchIsRetried() {
        consumer.step();
        publish(completion(5, ID_1, "SKU-A"));
        publish(completion(5, ID_2, "SKU-B"));
        when(stockCommits.applyBatch(anyList(), anyLong())).thenThrow(new IllegalStateException("inventory_db down"));

        assertThatThrownBy(consumer::step).isInstanceOf(IllegalStateException.class);

        assertThat(kafka.position(TP)).isEqualTo(10);
        verify(repository, never()).saveNextOffset(anyLong());
    }
}
