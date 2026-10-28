package com.school.selfcheckout.service.checkout;

import java.time.Instant;

/**
 * One row of checkout's outbox: an event waiting to be published.
 *
 * @param topic   the Kafka topic it goes to
 * @param type    the payload's type name, e.g. {@code ScanRecorded}
 * @param key     the record key, or null
 * @param payload the event body as JSON, built in SQL by the statement that wrote the row
 */
public record OutboxEvent(long seq, String topic, String type, String key, String payload, Instant createdAt) {
}
