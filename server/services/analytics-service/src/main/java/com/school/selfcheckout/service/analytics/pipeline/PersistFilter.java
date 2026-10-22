package com.school.selfcheckout.service.analytics.pipeline;

import com.school.selfcheckout.service.analytics.PopularityRepository;

/**
 * Stage 3, the sink: writes each ranked window to Postgres, where
 * {@code GET /analytics/popular-items} reads the latest one.
 *
 * Every window is written tagged with the epoch of the Reset that preceded
 * it; the repository discards it if admin-service has reset the database
 * since. In the monolith the reset call drained this pipeline directly -- a
 * separate admin-service cannot, so the database arbitrates instead.
 */
final class PersistFilter extends Filter<RankedWindow, Void> {

    private final PopularityRepository repository;
    private final int windowSize;
    private final int slideInterval;

    /** -1 until the first Reset; nothing is persisted before then. */
    private long epoch = -1;

    PersistFilter(Pipe<RankedWindow> input, PopularityRepository repository, int windowSize, int slideInterval) {
        super("persist", input, null);
        this.repository = repository;
        this.windowSize = windowSize;
        this.slideInterval = slideInterval;
    }

    @Override
    protected void process(RankedWindow window) {
        if (epoch < 0) {
            return;
        }
        repository.saveWindow(
                epoch, window.windowStart(), window.windowEnd(), windowSize, slideInterval, window.entries());
    }

    @Override
    protected void onReset(long epoch) {
        this.epoch = epoch;
    }
}
