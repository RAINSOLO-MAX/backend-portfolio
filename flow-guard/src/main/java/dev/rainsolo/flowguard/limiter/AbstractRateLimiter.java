package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;
import dev.rainsolo.flowguard.metrics.LimiterMetrics;
import dev.rainsolo.flowguard.metrics.LimiterStats;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/** Shared clock, rate, atomic counters and the blocking acquire loop for concrete algorithms. */
public abstract class AbstractRateLimiter implements RateLimiter {

    protected final Clock clock;
    protected final LimiterConfig config;
    protected final double permitsPerSecond;
    protected final LimiterMetrics metrics = new LimiterMetrics();
    private final AtomicLong allowedPermitCount = new AtomicLong();

    protected AbstractRateLimiter(LimiterConfig config, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.permitsPerSecond = config.permitsPerSecond();
    }

    protected static void checkPermits(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive");
        }
    }

    protected final void recordAllowed(int permits) {
        allowedPermitCount.addAndGet(permits);
        metrics.incAllowed(permits);
    }

    protected final void recordRejected(int permits) {
        metrics.incRejected(permits);
    }

    protected final long totalAllowedPermits() {
        return allowedPermitCount.get();
    }

    @Override
    public final boolean tryAcquire(String key) {
        return tryAcquire(key, 1).allowed();
    }

    @Override
    public final double acquire(String key, int permits) throws InterruptedException {
        checkPermits(permits);
        return AcquireSupport.acquire(clock, config, this, key, permits);
    }

    @Override
    public final LimiterStats stats() {
        LimiterMetrics.Snapshot snapshot = metrics.snapshot();
        return new LimiterStats(snapshot.allowedRequests(), snapshot.rejectedRequests(),
                snapshot.allowedPermits(), snapshot.rejectedPermits(), availablePermits());
    }

    /** Permits currently grantable without waiting. */
    protected abstract long availablePermits();
}
