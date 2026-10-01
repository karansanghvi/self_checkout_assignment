package com.school.selfcheckout.analytics.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * One stage of the pipeline: takes messages from its input pipe, transforms
 * each payload, and emits results to its output pipe. Runs on its own thread.
 *
 * A filter knows nothing about its neighbours beyond the pipes it was handed,
 * and its state is touched only by its own thread -- so no filter needs a lock.
 *
 * A filter with no output pipe is the sink: it is where Reset is acknowledged.
 */
abstract class Filter<I, O> implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Filter.class);

    private static final long CONTROL_FORWARD_TIMEOUT_SECONDS = 5;

    private final String name;
    private final Pipe<I> input;
    private final Pipe<O> output;

    Filter(String name, Pipe<I> input, Pipe<O> output) {
        this.name = name;
        this.input = input;
        this.output = output;
    }

    /** Handles one payload, calling {@link #emit} for each result (zero or more). */
    protected abstract void process(I payload);

    /** Clears this filter's state. Called on the filter's own thread, in order with the data. */
    protected void onReset() {
    }

    protected final void emit(O result) {
        output.send(result);
    }

    String name() {
        return name;
    }

    @Override
    public final void run() {
        try {
            while (true) {
                switch (input.take()) {
                    case Message.Data<I> data -> {
                        try {
                            process(data.payload());
                        } catch (RuntimeException e) {
                            // One bad payload must not kill the stage.
                            log.warn("Filter '{}' failed on a message: {}", name, e.toString());
                        }
                    }
                    case Message.Reset<I> reset -> {
                        onReset();
                        if (output == null) {
                            reset.done().countDown();
                        } else {
                            forward(new Message.Reset<>(reset.done()));
                        }
                    }
                    case Message.Stop<I> stop -> {
                        if (output != null) {
                            forward(new Message.Stop<>());
                        }
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void forward(Message<O> control) throws InterruptedException {
        if (!output.sendControl(control, CONTROL_FORWARD_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            log.warn("Filter '{}' could not forward {} to pipe '{}'",
                    name, control.getClass().getSimpleName(), output.name());
        }
    }
}
