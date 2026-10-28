package com.school.selfcheckout.messaging;

/**
 * The Kafka topics, created by {@code scripts/kafka-up.sh}; the broker does
 * not auto-create topics, so a misspelt name fails loudly.
 *
 * An enum rather than string constants on purpose: a {@code static final
 * String} is inlined by the compiler, which would hide from the architecture
 * tests which service uses which topic. An enum constant is a real field
 * access they can see.
 *
 * Every topic has one partition. For scans that is a requirement -- the
 * popularity window needs one global order. For the rest, the volume is small
 * or, for completions, consumed by a single batching consumer anyway.
 */
public enum Topic {
    SCAN_EVENTS("scan-events"),
    TRANSACTION_COMPLETED("transaction-completed"),
    /** Where inventory parks a completion it cannot apply, so it does not block the partition. */
    TRANSACTION_COMPLETED_DLT("transaction-completed.DLT"),
    /** Compacted: always holds the latest catalog. */
    CATALOG_SNAPSHOTS("catalog-snapshots"),
    RESET_REQUESTED("reset-requested"),
    RESET_COMPLETED("reset-completed");

    private final String topicName;

    Topic(String topicName) {
        this.topicName = topicName;
    }

    public String topicName() {
        return topicName;
    }
}
