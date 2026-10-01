package com.school.selfcheckout.analytics.pipeline;

import com.school.selfcheckout.repository.PopularityRepository;

/**
 * Stage 3, the sink: writes each ranked window to Postgres, where
 * {@code GET /analytics/popular-items} reads the latest one.
 */
final class PersistFilter extends Filter<RankedWindow, Void> {

    private final PopularityRepository repository;
    private final int windowSize;
    private final int slideInterval;

    PersistFilter(Pipe<RankedWindow> input, PopularityRepository repository, int windowSize, int slideInterval) {
        super("persist", input, null);
        this.repository = repository;
        this.windowSize = windowSize;
        this.slideInterval = slideInterval;
    }

    @Override
    protected void process(RankedWindow window) {
        repository.saveWindow(window.windowStart(), window.windowEnd(), windowSize, slideInterval, window.entries());
    }
}
