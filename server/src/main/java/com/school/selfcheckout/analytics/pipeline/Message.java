package com.school.selfcheckout.analytics.pipeline;

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

    /** Clears each filter's state in turn; the sink counts {@code done} down once it arrives. */
    record Reset<T>(CountDownLatch done) implements Message<T> {
    }

    /** Ends each filter's thread after forwarding itself downstream. */
    record Stop<T>() implements Message<T> {
    }
}
