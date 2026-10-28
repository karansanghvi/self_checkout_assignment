package com.school.selfcheckout.messaging;

import com.school.selfcheckout.domain.CheckoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One consumer, one thread, polling forever: the shape of every Kafka
 * consumer in the system.
 *
 * KafkaConsumer is not thread-safe, and some consumers' state (an epoch, a
 * position, a paused partition) must only change between polls. So work that
 * originates on another thread -- typically a reset arriving on a different
 * topic -- is handed in with {@link #callOnLoop}, which queues it, wakes the
 * consumer, and waits until the loop thread has run it.
 *
 * Errors are logged and retried after a backoff. The topic holds the backlog
 * durably, so a failure costs latency, never data.
 */
public abstract class KafkaPollLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPollLoop.class);

    static final long ERROR_BACKOFF_MILLIS = 1_000;

    private record Task(Runnable work, CompletableFuture<Void> done) {
    }

    protected final Consumer<String, String> consumer;
    private final String threadName;
    private final ConcurrentLinkedQueue<Task> tasks = new ConcurrentLinkedQueue<>();
    private boolean initialised;
    private volatile boolean running;
    private Thread thread;

    protected KafkaPollLoop(String threadName, Consumer<String, String> consumer) {
        this.threadName = threadName;
        this.consumer = consumer;
    }

    /** Assign partitions and seek. Runs once, on the loop thread, before the first poll. */
    protected abstract void init();

    /** One {@code consumer.poll} and the handling of what it returned. */
    protected abstract void poll();

    public synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        thread = new Thread(this, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        running = false;
        if (thread != null) {
            consumer.wakeup();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
        // The consumer itself is closed by whoever created it.
    }

    @Override
    public void run() {
        while (running) {
            try {
                step();
            } catch (WakeupException e) {
                // A task or stop() wants the thread's attention; the loop handles both.
            } catch (RuntimeException e) {
                log.warn("{} pass failed, retrying: {}", threadName, e.getMessage());
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
     * One iteration: initialise if needed, run queued tasks, poll. Public so
     * tests can drive a loop deterministically without its thread.
     */
    public final void step() {
        if (!initialised) {
            init();
            initialised = true;
        }
        Task task;
        while ((task = tasks.poll()) != null) {
            try {
                task.work().run();
                task.done().complete(null);
            } catch (RuntimeException e) {
                task.done().completeExceptionally(e);
            }
        }
        poll();
    }

    /**
     * Runs {@code work} on the loop thread, between polls, and waits for it.
     *
     * @throws CheckoutException DEPENDENCY_FAILED if it failed or did not run in time
     */
    public void callOnLoop(Runnable work, Duration timeout) {
        Task task = new Task(work, new CompletableFuture<>());
        tasks.add(task);
        consumer.wakeup();
        try {
            task.done().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CheckoutException.dependencyFailed("Interrupted waiting for " + threadName);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw CheckoutException.dependencyFailed(threadName + " failed: " + cause.getMessage());
        } catch (TimeoutException e) {
            throw CheckoutException.dependencyFailed(
                    threadName + " did not respond within " + timeout.toSeconds() + "s (is Kafka up?)");
        }
    }
}
