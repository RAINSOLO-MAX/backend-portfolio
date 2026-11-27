package dev.rainsolo.idemlock.lock;

import java.util.concurrent.atomic.AtomicLong;

/** Counters for lock acquisitions, releases, renewals, rejections and lease expiries. */
public final class LockMetrics {

    private final AtomicLong acquired = new AtomicLong();
    private final AtomicLong released = new AtomicLong();
    private final AtomicLong renewed = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong expired = new AtomicLong();

    public void incAcquired() {
        acquired.incrementAndGet();
    }

    public void incReleased() {
        released.incrementAndGet();
    }

    public void incRenewed() {
        renewed.incrementAndGet();
    }

    public void incRejected() {
        rejected.incrementAndGet();
    }

    public void incExpired() {
        expired.incrementAndGet();
    }

    public void addExpired(long delta) {
        if (delta > 0) {
            expired.addAndGet(delta);
        }
    }

    /** Immutable point-in-time view of all counters. */
    public record Snapshot(long acquired, long released, long renewed, long rejected, long expired) {
    }

    public Snapshot snapshot() {
        return new Snapshot(acquired.get(), released.get(), renewed.get(), rejected.get(), expired.get());
    }

    @Override
    public String toString() {
        return "LockMetrics" + snapshot();
    }
}
