package com.school.selfcheckout.service.checkout;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tails the outbox ({@code scan_events}) and publishes each scan, in seq
 * order, to Kafka -- the second half of the transactional-outbox pattern.
 *
 * In week 4 analytics-service polled this table itself, through the shared
 * database. It cannot reach checkout_db any more, so checkout publishes
 * instead, and the two services share only a topic.
 *
 * <b>Delivery.</b> At least once. The relay's position is saved to
 * {@code relay_state} only after Kafka has acknowledged the batch, so a crash
 * in between republishes it; analytics drops the repeated seqs.
 *
 * <b>Gaps.</b> A BIGSERIAL hands out numbers at insert time, but concurrent
 * inserts can commit out of order, so seq 101 may be visible before seq 100.
 * Publishing past 100 would skip it for good. The relay therefore publishes
 * only a contiguous run of seqs and, at a gap, waits for it to fill. A seq
 * still missing after {@link #GAP_TIMEOUT_NANOS} belonged to an insert that
 * rolled back and will never appear; it is skipped and counted. (This logic
 * is week 4's ScanFeed, moved to the side of the boundary that owns the table.)
 *
 * <b>Resets.</b> {@link #reset} takes the same lock as a publishing pass, so
 * a batch from the old epoch can never be published under the new one.
 */
@Component
public class OutboxRelay implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    static final int BATCH_SIZE = 5_000;
    static final long IDLE_POLL_MILLIS = 20;
    static final long ERROR_BACKOFF_MILLIS = 1_000;
    static final long GAP_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final OutboxRepository outbox;
    private final ScanPublisher publisher;
    private final long gapTimeoutNanos;
    private final Object lock = new Object();

    // Guarded by lock.
    private long epoch = -1;
    private long lastSeq = 0;
    private long gapSeenAt = 0;

    private final AtomicLong skippedSeqs = new AtomicLong();
    private volatile boolean running;
    private Thread thread;

    @Autowired
    public OutboxRelay(OutboxRepository outbox, ScanPublisher publisher) {
        this(outbox, publisher, GAP_TIMEOUT_NANOS);
    }

    OutboxRelay(OutboxRepository outbox, ScanPublisher publisher, long gapTimeoutNanos) {
        this.outbox = outbox;
        this.publisher = publisher;
        this.gapTimeoutNanos = gapTimeoutNanos;
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        thread = new Thread(this, "outbox-relay");
        thread.setDaemon(true);
        thread.start();
    }

    @PreDestroy
    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (!publishOnce()) {
                    Thread.sleep(IDLE_POLL_MILLIS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // Kafka or checkout_db unreachable. The outbox holds the backlog durably.
                log.warn("Outbox relay pass failed, retrying: {}", e.getMessage());
                try {
                    Thread.sleep(ERROR_BACKOFF_MILLIS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * Publishes whatever contiguous prefix of the next batch is safe to.
     *
     * @return true if there is probably more to publish right away
     */
    boolean publishOnce() {
        synchronized (lock) {
            if (epoch < 0) {
                RelayState state = outbox.loadState();
                epoch = state.epoch();
                lastSeq = state.publishedSeq();
            }

            List<OutboxEvent> batch = outbox.poll(lastSeq, BATCH_SIZE);
            int ready = 0;
            long next = lastSeq;
            for (OutboxEvent event : batch) {
                long expected = next + 1;
                if (event.seq() != expected) {
                    long now = System.nanoTime();
                    if (gapSeenAt == 0) {
                        gapSeenAt = now;
                    }
                    if (now - gapSeenAt < gapTimeoutNanos) {
                        break; // Wait for the missing seq to commit.
                    }
                    long missing = event.seq() - expected;
                    skippedSeqs.addAndGet(missing);
                    log.warn("Skipping {} outbox seq(s) {}..{} that never committed",
                            missing, expected, event.seq() - 1);
                }
                gapSeenAt = 0;
                next = event.seq();
                ready++;
            }
            if (ready == 0) {
                return false;
            }

            publisher.publish(epoch, batch.subList(0, ready));
            outbox.savePublishedSeq(next);
            lastSeq = next;
            return ready == BATCH_SIZE;
        }
    }

    /**
     * Wipes checkout_db's run state and starts {@code newEpoch}. Waits for any
     * publishing pass in progress to finish first.
     */
    public void reset(long newEpoch) {
        synchronized (lock) {
            outbox.resetTo(newEpoch);
            epoch = newEpoch;
            lastSeq = 0;
            gapSeenAt = 0;
        }
    }

    /** Highest seq published so far in the current epoch. */
    long lastSeq() {
        synchronized (lock) {
            return lastSeq;
        }
    }

    /** Seqs given up on as rolled back. Expected to stay at zero. */
    long skippedSeqs() {
        return skippedSeqs.get();
    }
}
