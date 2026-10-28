package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.contract.Events.ResetCompleted;
import com.school.selfcheckout.contract.Events.ResetRequested;
import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.domain.ErrorCode;
import com.school.selfcheckout.messaging.EventCodec;
import com.school.selfcheckout.messaging.EventPublisher;
import com.school.selfcheckout.messaging.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * The reset is a protocol over two topics; these pin its request, how acks
 * are matched to it, and its failure modes. Kafka's MockConsumer stands in
 * for reset-completed, and a fake publisher plays the four services.
 */
class ResetServiceTest {

    private static final List<String> PARTICIPANTS =
            List.of("catalog-service", "inventory-service", "checkout-service", "analytics-service");
    private static final TopicPartition ACKS = new TopicPartition(Topic.RESET_COMPLETED.topicName(), 0);

    private final EventPublisher publisher = mock(EventPublisher.class);
    private final List<MockConsumer<String, String>> consumers = new ArrayList<>();
    private long nextOffset = 3; // the topic already holds acks from earlier resets
    private long positionAtPublish = -1;

    /** What the services answer with, given the request's epoch. */
    private java.util.function.LongFunction<List<String>> answers =
            epoch -> PARTICIPANTS.stream().map(s -> ack(epoch, s, true, null)).toList();

    private ResetService newService(int catalogSize, int stockPerItem, long timeoutMs) {
        doAnswer(inv -> {
            long epoch = inv.getArgument(2);
            MockConsumer<String, String> consumer = consumers.get(consumers.size() - 1);
            positionAtPublish = consumer.position(ACKS);
            for (String value : answers.apply(epoch)) {
                consumer.addRecord(new ConsumerRecord<>(ACKS.topic(), 0, nextOffset++, null, value));
            }
            return null;
        }).when(publisher).publish(eq(Topic.RESET_REQUESTED), isNull(), anyLong(), any());

        return new ResetService(publisher, () -> {
            MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            consumer.updateBeginningOffsets(Map.of(ACKS, 0L));
            consumer.updateEndOffsets(Map.of(ACKS, nextOffset));
            consumers.add(consumer);
            return consumer;
        }, PARTICIPANTS, timeoutMs, catalogSize, stockPerItem);
    }

    private static String ack(long epoch, String service, boolean ok, String error) {
        return EventCodec.encode(epoch, 0, Instant.now(), new ResetCompleted(service, ok, error, 1));
    }

    @Test
    void publishesOneRequestCarryingTheResolvedSizes() {
        newService(1234, 99, 1000).reset(7, null);

        ArgumentCaptor<Object> request = ArgumentCaptor.forClass(Object.class);
        verify(publisher).publish(eq(Topic.RESET_REQUESTED), isNull(), anyLong(), request.capture());
        assertThat(request.getValue()).isEqualTo(new ResetRequested(7, 99));
    }

    @Test
    void waitsForEveryParticipantAndReportsTheirAcks() {
        Map<String, Object> result = newService(2000, 10000, 1000).reset();

        assertThat(result).containsEntry("status", "ok")
                .containsEntry("catalogSize", 2000)
                .containsEntry("stockPerItem", 10000);
        assertThat((List<?>) result.get("services")).hasSize(4).doesNotContainNull();
    }

    @Test
    void isPositionedAtTheEndOfTheAckTopicBeforeItPublishes() {
        newService(2000, 10000, 1000).reset();

        // Offsets 0..2 belong to earlier resets; this reset's acks start at 3.
        assertThat(positionAtPublish).isEqualTo(3);
        assertThat(consumers.get(0).closed()).isTrue();
    }

    @Test
    void acksForAnotherEpochAndDuplicatesAreIgnored() {
        answers = epoch -> List.of(
                ack(epoch - 1, "catalog-service", true, null),
                ack(epoch, "catalog-service", true, null),
                ack(epoch, "catalog-service", true, null),
                ack(epoch, "inventory-service", true, null),
                "garbage",
                ack(epoch, "checkout-service", true, null),
                ack(epoch, "analytics-service", true, null));

        assertThat((List<?>) newService(2000, 10000, 1000).reset().get("services")).hasSize(4);
    }

    @Test
    void aFailedAckFailsTheResetAndNamesTheService() {
        answers = epoch -> List.of(
                ack(epoch, "catalog-service", true, null),
                ack(epoch, "inventory-service", false, "inventory_db unreachable"));

        assertThatThrownBy(newService(2000, 10000, 1000)::reset)
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("inventory-service")
                .hasMessageContaining("inventory_db unreachable")
                .extracting(e -> ((CheckoutException) e).getErrorCode())
                .isEqualTo(ErrorCode.DEPENDENCY_FAILED);
    }

    @Test
    void aMissingAckTimesOutAndNamesWhoDidNotAnswer() {
        answers = epoch -> List.of(
                ack(epoch, "catalog-service", true, null),
                ack(epoch, "inventory-service", true, null),
                ack(epoch, "checkout-service", true, null));

        assertThatThrownBy(newService(2000, 10000, 300)::reset)
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("[analytics-service]");
    }

    @Test
    void epochsIncreaseFromOneResetToTheNext() {
        ResetService service = newService(2000, 10000, 1000);
        long first = (long) service.reset().get("epoch");
        long second = (long) service.reset().get("epoch");

        assertThat(second).isGreaterThan(first);
    }

    @Test
    void invalidSizesAreRefusedBeforeAnythingIsPublished() {
        ResetService service = newService(2000, 10000, 1000);

        assertThatThrownBy(() -> service.reset(0, null))
                .extracting(e -> ((CheckoutException) e).getErrorCode())
                .isEqualTo(ErrorCode.INVALID_REQUEST);
        assertThat(consumers).isEmpty();
    }
}
