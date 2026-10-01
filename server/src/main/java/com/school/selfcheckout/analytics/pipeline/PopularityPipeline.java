package com.school.selfcheckout.analytics.pipeline;

import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.repository.PopularityRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Wires the windowed-analytics pipeline and owns its threads:
 *
 * <pre>
 *   scan()  --scanPipe-->  window  --windowPipe-->  rank  --rankedPipe-->  persist  --> Postgres
 * </pre>
 *
 * <ul>
 *   <li><b>window</b> buffers scans in a ring and emits a snapshot every slideInterval scans</li>
 *   <li><b>rank</b> counts SKUs in a snapshot and keeps the top N</li>
 *   <li><b>persist</b> writes each ranked window to the database</li>
 * </ul>
 *
 * Each filter runs on its own thread; the pipes are bounded
 * {@link java.util.concurrent.ArrayBlockingQueue}s. The scan pipe is deep and
 * drops new scans when full, so a request thread never waits on analytics. The
 * snapshot pipes are shallow and drop the oldest snapshot when full, since a
 * newer snapshot supersedes it.
 */
@Component
public class PopularityPipeline {

    private static final Logger log = LoggerFactory.getLogger(PopularityPipeline.class);

    /** Persist more ranks than the default limit=10 so larger queries can be served from a snapshot. */
    static final int MAX_PERSISTED_RANKS = 50;

    static final int SCAN_PIPE_CAPACITY = 65_536;
    static final int SNAPSHOT_PIPE_CAPACITY = 4;

    private static final long CONTROL_TIMEOUT_SECONDS = 10;

    private final Pipe<String> scanPipe;
    private final List<Pipe<?>> pipes;
    private final WindowFilter windowFilter;
    private final List<Filter<?, ?>> filters;
    private final List<Thread> threads = new ArrayList<>();

    public PopularityPipeline(PopularityRepository repository, AppProperties properties) {
        int windowSize = Math.max(1, properties.getPopularity().getWindowSize());
        int slideInterval = Math.max(1, properties.getPopularity().getSlideInterval());

        this.scanPipe = new Pipe<>("scans", SCAN_PIPE_CAPACITY, Pipe.Overflow.DROP_NEWEST);
        Pipe<WindowSnapshot> windowPipe = new Pipe<>("windows", SNAPSHOT_PIPE_CAPACITY, Pipe.Overflow.DROP_OLDEST);
        Pipe<RankedWindow> rankedPipe = new Pipe<>("ranked", SNAPSHOT_PIPE_CAPACITY, Pipe.Overflow.DROP_OLDEST);
        this.pipes = List.of(scanPipe, windowPipe, rankedPipe);

        this.windowFilter = new WindowFilter(scanPipe, windowPipe, windowSize, slideInterval);
        this.filters = List.of(
                windowFilter,
                new RankFilter(windowPipe, rankedPipe, MAX_PERSISTED_RANKS),
                new PersistFilter(rankedPipe, repository, windowSize, slideInterval));
    }

    @PostConstruct
    public synchronized void start() {
        if (!threads.isEmpty()) {
            return;
        }
        for (Filter<?, ?> filter : filters) {
            Thread t = new Thread(filter, "pipeline-" + filter.name());
            t.setDaemon(true);
            t.start();
            threads.add(t);
        }
    }

    /** Hands one scan to the pipeline. Never blocks. */
    public void submit(String sku) {
        scanPipe.send(sku);
    }

    /**
     * Clears every filter's state and waits until the Reset has reached the
     * sink, so on return nothing from before the reset is still in flight.
     */
    public void reset() {
        CountDownLatch done = new CountDownLatch(1);
        try {
            boolean sent = scanPipe.sendControl(new Message.Reset<>(done), CONTROL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!sent || !done.await(CONTROL_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Popularity pipeline did not acknowledge reset within {}s", CONTROL_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Scans the window filter has consumed since the last reset. */
    public long currentSequence() {
        return windowFilter.currentSequence();
    }

    /** Total messages shed by all pipes since startup. Expected to stay at zero. */
    public long droppedMessages() {
        return pipes.stream().mapToLong(Pipe::dropped).sum();
    }

    @PreDestroy
    public synchronized void stop() {
        try {
            if (!scanPipe.sendControl(new Message.Stop<>(), CONTROL_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                threads.forEach(Thread::interrupt);
            }
            for (Thread t : threads) {
                t.join(TimeUnit.SECONDS.toMillis(CONTROL_TIMEOUT_SECONDS));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (Pipe<?> pipe : pipes) {
            if (pipe.dropped() > 0) {
                log.warn("Pipe '{}' dropped {} message(s) during this run", pipe.name(), pipe.dropped());
            }
        }
    }
}
