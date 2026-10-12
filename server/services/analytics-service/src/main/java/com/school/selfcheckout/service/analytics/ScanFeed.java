package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.data.analytics.ScanBatch;
import com.school.selfcheckout.data.analytics.ScanEvent;
import com.school.selfcheckout.data.analytics.ScanEventRepository;
import com.school.selfcheckout.service.analytics.pipeline.PopularityPipeline;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The pipeline's data source: tails {@code scan_events} and submits each scan,
 * in seq order, to the {@link PopularityPipeline}.
 *
 * In the monolith, checkout's request threads pushed scans straight into the
 * pipeline. Here checkout-service writes them to the shared table and this
 * single thread reads them back, so the two services never exchange a message.
 *
 * <b>Resets.</b> Every poll also returns the current epoch, read in the same
 * statement as the events. When it changes, admin-service has wiped the
 * database: the pipeline is reset under the new epoch and the feed restarts at
 * seq 0 ({@code TRUNCATE ... RESTART IDENTITY} rewinds the sequence). The
 * first poll after startup is treated the same way, which replays the current
 * epoch's log and rebuilds the window -- a restarted analytics-service loses
 * nothing.
 *
 * <b>Gaps.</b> A BIGSERIAL hands out numbers at insert time, but concurrent
 * inserts can commit out of order, so seq 101 may be visible before seq 100.
 * Reading past 100 would skip it for good. The feed therefore consumes only a
 * contiguous run of seqs and, at a gap, waits for it to fill. A seq that is
 * still missing after {@link #GAP_TIMEOUT_NANOS} belonged to an insert that
 * rolled back and will never appear; it is skipped and counted.
 */
@Component
public class ScanFeed implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ScanFeed.class);

    static final int BATCH_SIZE = 5_000;
    static final long IDLE_POLL_MILLIS = 50;
    static final long ERROR_BACKOFF_MILLIS = 1_000;
    static final long GAP_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final ScanEventRepository scanEvents;
    private final PopularityPipeline pipeline;
    private final long gapTimeoutNanos;

    // Touched only by the feed thread (or a test driving pollOnce directly).
    private long epoch = -1;
    private long lastSeq = 0;
    private long gapSeenAt = 0;

    private final AtomicLong skippedSeqs = new AtomicLong();
    private volatile boolean running;
    private Thread thread;

    @Autowired
    public ScanFeed(ScanEventRepository scanEvents, PopularityPipeline pipeline) {
        this(scanEvents, pipeline, GAP_TIMEOUT_NANOS);
    }

    ScanFeed(ScanEventRepository scanEvents, PopularityPipeline pipeline, long gapTimeoutNanos) {
        this.scanEvents = scanEvents;
        this.pipeline = pipeline;
        this.gapTimeoutNanos = gapTimeoutNanos;
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        thread = new Thread(this, "scan-feed");
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
                if (!pollOnce()) {
                    Thread.sleep(IDLE_POLL_MILLIS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (DataAccessException e) {
                log.warn("Scan feed poll failed, retrying: {}", e.getMessage());
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
     * Reads one batch and feeds whatever contiguous prefix of it is safe to
     * consume.
     *
     * @return true if there is probably more to read right away -- a full
     *         batch, or a reset that restarted the feed
     */
    boolean pollOnce() {
        ScanBatch batch = scanEvents.poll(lastSeq, BATCH_SIZE);

        if (batch.epoch() != epoch) {
            if (epoch != -1) {
                log.info("Database reset detected (epoch {} -> {}); restarting the scan feed", epoch, batch.epoch());
            }
            pipeline.reset(batch.epoch());
            epoch = batch.epoch();
            lastSeq = 0;
            gapSeenAt = 0;
            // The events in this batch were read relative to the old position; re-read from the start.
            return true;
        }

        int consumed = 0;
        for (ScanEvent event : batch.events()) {
            long expected = lastSeq + 1;
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
                log.warn("Skipping {} scan_events seq(s) {}..{} that never committed",
                        missing, expected, event.seq() - 1);
            }
            gapSeenAt = 0;
            pipeline.submit(event.sku());
            lastSeq = event.seq();
            consumed++;
        }
        return consumed == BATCH_SIZE;
    }

    /** Highest seq consumed so far in the current epoch. */
    long lastSeq() {
        return lastSeq;
    }

    /** Seqs given up on as rolled back. Expected to stay at zero. */
    long skippedSeqs() {
        return skippedSeqs.get();
    }
}
