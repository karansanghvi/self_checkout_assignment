package com.school.selfcheckout.analytics.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * A bounded, asynchronous connection between two filters, backed by an
 * {@link ArrayBlockingQueue}.
 *
 * Sending data never blocks. Analytics must never apply backpressure to
 * checkout, so when a pipe is full it sheds load according to its
 * {@link Overflow} policy and counts what it dropped. Control messages are the
 * exception: they are never dropped, and their sender waits (bounded) for room.
 */
final class Pipe<T> {

    private static final Logger log = LoggerFactory.getLogger(Pipe.class);

    enum Overflow {
        /** Refuse the incoming item. Right for raw scans, where every queued scan is still needed. */
        DROP_NEWEST,
        /** Evict the oldest queued item. Right for snapshots, which a newer one supersedes. */
        DROP_OLDEST
    }

    private final String name;
    private final BlockingQueue<Message<T>> queue;
    private final Overflow overflow;
    private final LongAdder dropped = new LongAdder();

    Pipe(String name, int capacity, Overflow overflow) {
        this.name = name;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.overflow = overflow;
    }

    /** Sends one payload without blocking, shedding load per the overflow policy if full. */
    void send(T payload) {
        Message<T> message = new Message.Data<>(payload);
        if (queue.offer(message)) {
            return;
        }
        if (overflow == Overflow.DROP_OLDEST) {
            while (dropOldestData()) {
                if (queue.offer(message)) {
                    return;
                }
            }
        }
        // DROP_NEWEST, or a queue holding nothing but control messages.
        recordDrop();
    }

    /**
     * Sends a control message, waiting up to {@code timeout} for room.
     *
     * @return false if the pipe stayed full for the whole timeout
     */
    boolean sendControl(Message<T> message, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.offer(message, timeout, unit);
    }

    Message<T> take() throws InterruptedException {
        return queue.take();
    }

    long dropped() {
        return dropped.sum();
    }

    String name() {
        return name;
    }

    /**
     * Evicts the oldest data message, skipping over control messages: losing a
     * stale snapshot is harmless, losing a Reset or Stop is not.
     */
    private boolean dropOldestData() {
        Iterator<Message<T>> it = queue.iterator();
        while (it.hasNext()) {
            if (it.next() instanceof Message.Data<T>) {
                it.remove();
                recordDrop();
                return true;
            }
        }
        return false;
    }

    private void recordDrop() {
        dropped.increment();
        long total = dropped.sum();
        if (total == 1 || total % 10_000 == 0) {
            log.warn("Pipe '{}' is full; {} message(s) dropped so far", name, total);
        }
    }
}
