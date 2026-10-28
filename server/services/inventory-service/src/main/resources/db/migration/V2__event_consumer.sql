-- Week 6: stock commits arrive as TransactionCompleted events on Kafka
-- instead of as HTTP calls from checkout-service.

-- Single row: the reset epoch inventory_db is in, and the next offset of the
-- transaction-completed topic to apply. The offset is saved in the same
-- database transaction as the stock changes of the records before it, so a
-- crash can never apply a record twice or skip one: on restart the consumer
-- seeks to exactly where the committed state ends.
CREATE TABLE inventory_state (
    id          INT    PRIMARY KEY CHECK (id = 1),
    epoch       BIGINT NOT NULL,
    next_offset BIGINT NOT NULL
);

INSERT INTO inventory_state (id, epoch, next_offset) VALUES (1, 0, 0);
