package dev.rainsolo.delayqueue.metrics;

import java.util.concurrent.atomic.AtomicLong;

public final class QueueMetrics {

    public record Snapshot(long submitted, long expired, long succeeded, long retried,
                           long deadLettered, long duplicatesSuppressed) {
    }

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong expired = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private final AtomicLong duplicatesSuppressed = new AtomicLong();

    public void incSubmitted() {
        submitted.incrementAndGet();
    }

    public void incExpired() {
        expired.incrementAndGet();
    }

    public void incSucceeded() {
        succeeded.incrementAndGet();
    }

    public void incRetried() {
        retried.incrementAndGet();
    }

    public void incDeadLettered() {
        deadLettered.incrementAndGet();
    }

    public void incDuplicateSuppressed() {
        duplicatesSuppressed.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(submitted.get(), expired.get(), succeeded.get(), retried.get(),
                deadLettered.get(), duplicatesSuppressed.get());
    }
}
