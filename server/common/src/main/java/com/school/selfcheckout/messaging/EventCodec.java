package com.school.selfcheckout.messaging;

import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.school.selfcheckout.contract.Events.Envelope;

import java.time.Instant;
import java.util.UUID;

/**
 * JSON encoding of {@link Envelope}s.
 *
 * Its own ObjectMapper rather than Spring's, so the event format cannot drift
 * with a service's HTTP serialisation settings. Unknown fields are ignored,
 * so a producer can add a field without breaking a consumer that has not
 * been redeployed yet.
 */
public final class EventCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private EventCodec() {
    }

    /** Wraps {@code payload} for publishing. The type is the payload's record name. */
    public static String encode(long epoch, long seq, Instant occurredAt, Object payload) {
        String type = payload.getClass().getSimpleName();
        String eventId = seq > 0 ? epoch + "-" + seq : UUID.randomUUID().toString();
        return encode(new Envelope(eventId, type, epoch, seq, occurredAt, MAPPER.valueToTree(payload)));
    }

    /**
     * The same wire format as {@link Envelope}, with the payload spliced in
     * as-is. Postgres built that JSON (json_build_object), so it is already
     * valid; parsing it only to write it straight back out would cost the
     * relay a parse per scan, thousands of times a second.
     */
    private record RawEnvelope(String eventId, String type, long epoch, long seq, Instant occurredAt,
                               @JsonRawValue String payload) {
    }

    /** For a payload that is already JSON, e.g. one built in SQL by checkout's outbox. */
    public static String encodeRaw(String eventId, String type, long epoch, long seq, Instant occurredAt,
                                   String payloadJson) {
        try {
            return MAPPER.writeValueAsString(new RawEnvelope(eventId, type, epoch, seq, occurredAt, payloadJson));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not encode " + type, e);
        }
    }

    public static String encode(Envelope envelope) {
        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not encode " + envelope.type(), e);
        }
    }

    /** @throws MalformedEventException if the value is not an envelope */
    public static Envelope decode(String value) {
        if (value == null) {
            throw new MalformedEventException("null record value");
        }
        try {
            Envelope envelope = MAPPER.readValue(value, Envelope.class);
            if (envelope.type() == null || envelope.payload() == null) {
                throw new MalformedEventException("missing type or payload: " + value);
            }
            return envelope;
        } catch (JsonProcessingException e) {
            throw new MalformedEventException("not JSON: " + e.getOriginalMessage());
        }
    }

    /** @throws MalformedEventException if the payload is not a {@code payloadType} */
    public static <T> T payload(Envelope envelope, Class<T> payloadType) {
        if (!payloadType.getSimpleName().equals(envelope.type())) {
            throw new MalformedEventException(
                    "expected " + payloadType.getSimpleName() + ", got " + envelope.type());
        }
        try {
            return MAPPER.treeToValue(envelope.payload(), payloadType);
        } catch (JsonProcessingException e) {
            throw new MalformedEventException(envelope.type() + " payload unreadable: " + e.getOriginalMessage());
        }
    }

    /** A record that can never be processed, however often it is retried. */
    public static class MalformedEventException extends RuntimeException {
        public MalformedEventException(String message) {
            super(message);
        }
    }
}
