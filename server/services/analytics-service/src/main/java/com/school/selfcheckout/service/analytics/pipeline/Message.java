package com.school.selfcheckout.service.analytics.pipeline;

import java.util.concurrent.CountDownLatch;

/**
 * What actually travels down a {@link Pipe}: either a payload, or a control
 * signal.
 *
 * Control signals travel in-band, through the same queues as the data, so they
 * stay ordered with it. A Reset that overtook scans still in flight would let
 * the previous run leak into the next one; sent down the pipe, it reaches each
 * filter only after everything that was queued ahead of it.
 */
sealed interface Message<T> {

    record Data<T>(T payload) implements Message<T> {
    }

    /**
     * Clears each filter's state in turn; the sink counts {@code done} down
     * once it arrives. Carries the database epoch the data that follows
     * belongs to, so the sink can tag what it persists.
     */
    record Reset<T>(long epoch, CountDownLatch done) implements Message<T> {
    }

    /** Ends each filter's thread after forwarding itself downstream. */
    record Stop<T>() implements Message<T> {
    }
}
