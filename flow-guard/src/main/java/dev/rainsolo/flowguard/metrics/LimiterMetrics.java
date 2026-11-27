package dev.rainsolo.flowguard.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Lock free counters for allow/reject traffic. */
public final class LimiterMetrics {

    public record Snapshot(long allowedRequests, long rejectedRequests,
                           long allowedPermits, long rejectedPermits) {
    }

    private final AtomicLong allowedRequests = new AtomicLong();
    private final AtomicLong rejectedRequests = new AtomicLong();
    private final AtomicLong allowedPermits = new AtomicLong();
    private final AtomicLong rejectedPermits = new AtomicLong();

    public void incAllowed(long permits) {
        allowedRequests.incrementAndGet();
        allowedPermits.addAndGet(permits);
    }

    public void incRejected(long permits) {
        rejectedRequests.incrementAndGet();
        rejectedPermits.addAndGet(permits);
    }

    public Snapshot snapshot() {
        return new Snapshot(allowedRequests.get(), rejectedRequests.get(),
                allowedPermits.get(), rejectedPermits.get());
    }
}
