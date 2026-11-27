package dev.rainsolo.idemlock.executor;

import java.util.concurrent.atomic.AtomicLong;

/** Counters for idempotent executions, all updated with atomic increments. */
public final class IdempotentMetrics {

    private final AtomicLong executed = new AtomicLong();
    private final AtomicLong reused = new AtomicLong();
    private final AtomicLong running = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong expired = new AtomicLong();

    public void incExecuted() {
        executed.incrementAndGet();
    }

    public void incReused() {
        reused.incrementAndGet();
    }

    public void incRunning() {
        running.incrementAndGet();
    }

    public void incFailed() {
        failed.incrementAndGet();
    }

    public void addExpired(long delta) {
        if (delta > 0) {
            expired.addAndGet(delta);
        }
    }

    /** Immutable point-in-time view of all counters. */
    public record Snapshot(long executed, long reused, long running, long failed, long expired) {
    }

    public Snapshot snapshot() {
        return new Snapshot(executed.get(), reused.get(), running.get(), failed.get(), expired.get());
    }

    @Override
    public String toString() {
        return "IdempotentMetrics" + snapshot();
    }
}
