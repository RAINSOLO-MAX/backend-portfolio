package dev.rainsolo.scheduler.scheduler;

import java.util.concurrent.atomic.AtomicLong;

/** Atomic counters describing scheduler activity. */
public final class SchedulerMetrics {

    /** Immutable view of all counters. */
    public record Snapshot(long triggered, long shards, long misfired, long failover,
                           long duplicatesSuppressed, long failed, long retried) {
    }

    private final AtomicLong triggered = new AtomicLong();
    private final AtomicLong shards = new AtomicLong();
    private final AtomicLong misfired = new AtomicLong();
    private final AtomicLong failover = new AtomicLong();
    private final AtomicLong duplicatesSuppressed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();

    public void incTriggered() {
        triggered.incrementAndGet();
    }

    public void incShards() {
        shards.incrementAndGet();
    }

    public void incMisfired(long delta) {
        misfired.addAndGet(delta);
    }

    public void incFailover(long delta) {
        failover.addAndGet(delta);
    }

    public void incDuplicatesSuppressed() {
        duplicatesSuppressed.incrementAndGet();
    }

    public void incFailed() {
        failed.incrementAndGet();
    }

    public void incRetried() {
        retried.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(triggered.get(), shards.get(), misfired.get(), failover.get(),
                duplicatesSuppressed.get(), failed.get(), retried.get());
    }
}
