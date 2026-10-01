package com.school.selfcheckout.analytics.pipeline;

import java.util.Arrays;

/**
 * Stage 1: buffers scans into the hopping window.
 *
 * Keeps the most recent {@code windowSize} SKUs in a ring buffer and, every
 * {@code slideInterval} scans, emits a copy of the window. The copy is taken at
 * the exact scan that triggers the hop, so a snapshot always covers precisely
 * the scans its windowStart/windowEnd claim.
 */
final class WindowFilter extends Filter<String, WindowSnapshot> {

    private final int windowSize;
    private final int slideInterval;
    private final String[] ring;
    private long sequence = 0L;

    /** Written only by this filter's thread; read by request threads for the empty-window response. */
    private volatile long publishedSequence = 0L;

    WindowFilter(Pipe<String> input, Pipe<WindowSnapshot> output, int windowSize, int slideInterval) {
        super("window", input, output);
        this.windowSize = windowSize;
        this.slideInterval = slideInterval;
        this.ring = new String[windowSize];
    }

    @Override
    protected void process(String sku) {
        ring[(int) (sequence % windowSize)] = sku;
        sequence++;
        publishedSequence = sequence;

        if (sequence % slideInterval == 0) {
            int valid = (int) Math.min(sequence, windowSize);
            emit(new WindowSnapshot(
                    Math.max(0, sequence - windowSize),
                    sequence,
                    Arrays.copyOf(ring, valid)));
        }
    }

    @Override
    protected void onReset() {
        Arrays.fill(ring, null);
        sequence = 0L;
        publishedSequence = 0L;
    }

    long currentSequence() {
        return publishedSequence;
    }
}
