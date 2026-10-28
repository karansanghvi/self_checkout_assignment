package com.school.selfcheckout.messaging;

import com.school.selfcheckout.client.CatalogReplica;
import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Events.CatalogSnapshot;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Replication and the reset signal, driven through {@link CatalogFeed#step()} on a MockConsumer. */
class CatalogFeedTest {

    private static final TopicPartition TP = new TopicPartition(Topic.CATALOG_SNAPSHOTS.topicName(), 0);

    private final MockConsumer<String, String> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final CatalogReplica replica = new CatalogReplica();
    private final ResetAcknowledger acks = mock(ResetAcknowledger.class);
    private final List<Long> resets = new ArrayList<>();
    private long epoch = 5;
    private RuntimeException resetFailure;
    private long nextOffset = 0;

    private final CatalogFeed.ResetParticipant participant = new CatalogFeed.ResetParticipant() {
        @Override
        public long currentEpoch() {
            return epoch;
        }

        @Override
        public void resetTo(long newEpoch, CatalogSnapshot snapshot) {
            if (resetFailure != null) {
                throw resetFailure;
            }
            resets.add(newEpoch);
            epoch = newEpoch;
        }
    };

    private CatalogFeed feed;

    private static String snapshot(long epoch, String... skus) {
        List<CatalogItem> items = new ArrayList<>();
        for (String sku : skus) {
            items.add(new CatalogItem(sku, "Item " + sku, BigDecimal.ONE));
        }
        return EventCodec.encode(epoch, 0, Instant.EPOCH, new CatalogSnapshot(100, items));
    }

    private void publish(String value) {
        kafka.addRecord(new ConsumerRecord<>(TP.topic(), 0, nextOffset++, "catalog", value));
    }

    /** Snapshots already in the topic when the service starts. */
    private void existing(String... values) {
        kafka.updateBeginningOffsets(Map.of(TP, 0L));
        kafka.updateEndOffsets(Map.of(TP, (long) values.length));
        feed = new CatalogFeed(kafka, replica, participant, acks);
        feed.step(); // init + first poll: nothing added yet
        for (String value : values) {
            publish(value);
        }
    }

    @BeforeEach
    void setUp() {
        when(acks.service()).thenReturn("test-service");
    }

    @Test
    void onStartupOnlyTheLatestSnapshotIsHandled() {
        existing(snapshot(6, "A"), snapshot(7, "A", "B"));
        feed.step();

        assertThat(feed.isCaughtUp()).isTrue();
        assertThat(resets).containsExactly(7L); // one reset to the latest, not one per missed run
        verify(acks).ok(org.mockito.ArgumentMatchers.eq(7L), anyLong());
        assertThat(replica.findBySku("B")).isNotNull();
    }

    @Test
    void aSnapshotForTheCurrentEpochAtStartupLoadsTheReplicaWithoutResettingOrAcking() {
        existing(snapshot(5, "A"));
        feed.step();

        assertThat(resets).isEmpty();
        verify(acks, never()).ok(anyLong(), anyLong());
        assertThat(replica.findBySku("A")).isNotNull();
    }

    @Test
    void aLiveSnapshotFromANewerEpochResetsAndAcknowledges() {
        existing();
        publish(snapshot(6, "A"));
        feed.step();

        assertThat(resets).containsExactly(6L);
        verify(acks).ok(org.mockito.ArgumentMatchers.eq(6L), anyLong());
    }

    @Test
    void aLiveSnapshotForTheCurrentEpochIsAcknowledgedAgain() {
        existing();
        publish(snapshot(5, "A"));
        feed.step();

        assertThat(resets).isEmpty();
        verify(acks).ok(5, 0);
    }

    @Test
    void anOlderSnapshotIsIgnoredOnceTheReplicaIsLoaded() {
        existing(snapshot(5, "A"));
        feed.step();
        publish(snapshot(4, "OLD"));
        feed.step();

        assertThat(replica.findBySku("OLD")).isNull();
        assertThat(resets).isEmpty();
    }

    @Test
    void aFailedResetIsReportedAsAFailedAck() {
        existing();
        resetFailure = new IllegalStateException("db down");
        publish(snapshot(6, "A"));
        feed.step();

        verify(acks).failed(org.mockito.ArgumentMatchers.eq(6L), org.mockito.ArgumentMatchers.eq("db down"), anyLong());
        verify(acks, never()).ok(anyLong(), anyLong());
    }

    @Test
    void ifTheAckCannotBeSentTheSnapshotIsRetried() {
        existing();
        doThrow(new IllegalStateException("Kafka down")).when(acks).ok(6, 0);
        doThrow(new IllegalStateException("Kafka down")).when(acks).ok(org.mockito.ArgumentMatchers.eq(6L), anyLong());
        publish(snapshot(6, "A"));

        assertThatThrownBy(feed::step).isInstanceOf(IllegalStateException.class);
        assertThat(kafka.position(TP)).isZero(); // rewound to the snapshot
        assertThat(resets).containsExactly(6L);
    }

    @Test
    void malformedSnapshotsAreSkipped() {
        existing();
        publish("garbage");
        feed.step();

        verify(acks, never()).ok(anyLong(), anyLong());
        verify(acks, never()).failed(anyLong(), anyString(), anyLong());
    }
}
