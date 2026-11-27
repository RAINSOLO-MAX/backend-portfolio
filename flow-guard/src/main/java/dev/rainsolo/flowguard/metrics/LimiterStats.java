package dev.rainsolo.flowguard.metrics;

/** Immutable view of a limiter's counters plus the currently available permits. */
public record LimiterStats(long allowedRequests, long rejectedRequests,
                           long allowedPermits, long rejectedPermits,
                           long availablePermits) {

    public static LimiterStats empty() {
        return new LimiterStats(0L, 0L, 0L, 0L, 0L);
    }
}
