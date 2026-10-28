package com.school.selfcheckout.messaging;

import com.school.selfcheckout.contract.Events.CompletedLine;
import com.school.selfcheckout.contract.Events.Envelope;
import com.school.selfcheckout.contract.Events.ScanRecorded;
import com.school.selfcheckout.contract.Events.TransactionCompleted;
import com.school.selfcheckout.messaging.EventCodec.MalformedEventException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventCodecTest {

    private static final Instant AT = Instant.parse("2026-10-27T12:00:00Z");

    @Test
    void anEventRoundTrips() {
        Envelope envelope = EventCodec.decode(EventCodec.encode(7, 0, AT, new ScanRecorded("SKU-000001")));

        assertThat(envelope.type()).isEqualTo("ScanRecorded");
        assertThat(envelope.epoch()).isEqualTo(7);
        assertThat(envelope.occurredAt()).isEqualTo(AT);
        assertThat(EventCodec.payload(envelope, ScanRecorded.class)).isEqualTo(new ScanRecorded("SKU-000001"));
    }

    /** The relay's fast path must produce exactly what a consumer decodes as the typed event. */
    @Test
    void aRawPayloadFromTheOutboxDecodesLikeAnEncodedOne() {
        String sqlBuiltJson = "{\"transactionId\" : \"t-1\", \"stationId\" : \"s-1\", "
                + "\"lines\" : [{\"sku\" : \"SKU-A\", \"quantity\" : 2}]}";

        Envelope envelope = EventCodec.decode(
                EventCodec.encodeRaw("7-42", "TransactionCompleted", 7, 42, AT, sqlBuiltJson));

        assertThat(envelope.eventId()).isEqualTo("7-42");
        assertThat(envelope.seq()).isEqualTo(42);
        assertThat(EventCodec.payload(envelope, TransactionCompleted.class)).isEqualTo(
                new TransactionCompleted("t-1", "s-1", List.of(new CompletedLine("SKU-A", 2))));
    }

    @Test
    void aPayloadOfTheWrongTypeIsMalformed() {
        Envelope envelope = EventCodec.decode(EventCodec.encode(7, 0, AT, new ScanRecorded("SKU-A")));

        assertThatThrownBy(() -> EventCodec.payload(envelope, TransactionCompleted.class))
                .isInstanceOf(MalformedEventException.class);
    }

    @Test
    void somethingThatIsNotAnEnvelopeIsMalformed() {
        assertThatThrownBy(() -> EventCodec.decode("7:1:SKU-A")).isInstanceOf(MalformedEventException.class);
        assertThatThrownBy(() -> EventCodec.decode("{\"epoch\":1}")).isInstanceOf(MalformedEventException.class);
        assertThatThrownBy(() -> EventCodec.decode(null)).isInstanceOf(MalformedEventException.class);
    }
}
