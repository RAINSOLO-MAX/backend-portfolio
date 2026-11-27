package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;
import dev.rainsolo.flowguard.metrics.LimiterMetrics;
import dev.rainsolo.flowguard.metrics.LimiterStats;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** AND composition: every rule must allow. A rejection reports the strictest retry delay. */
public final class RuleChain implements RateLimiter {

    private final Clock clock;
    private final LimiterConfig config;
    private final List<RateLimiter> limiters;
    private final LimiterMetrics metrics = new LimiterMetrics();

    public RuleChain(Clock clock, LimiterConfig config, RateLimiter... limiters) {
        this(clock, config, Arrays.asList(limiters));
    }

    public RuleChain(Clock clock, LimiterConfig config, List<RateLimiter> limiters) {
        if (limiters == null || limiters.isEmpty()) {
            throw new IllegalArgumentException("a rule chain needs at least one limiter");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
        this.limiters = List.copyOf(limiters);
    }

    @Override
    public PermitResult tryAcquire(String key, int permits) {
        long minRemaining = Long.MAX_VALUE;
        long maxWait = 0;
        boolean allAllowed = true;
        for (RateLimiter limiter : limiters) {
            PermitResult result = limiter.tryAcquire(key, permits);
            if (result.allowed()) {
                minRemaining = Math.min(minRemaining, result.remainingPermits());
            } else {
                allAllowed = false;
                maxWait = Math.max(maxWait, result.waitMillis());
            }
        }
        if (allAllowed) {
            metrics.incAllowed(permits);
            return PermitResult.permit(minRemaining == Long.MAX_VALUE ? 0 : minRemaining);
        }
        metrics.incRejected(permits);
        return PermitResult.reject(maxWait);
    }

    @Override
    public boolean tryAcquire(String key) {
        return tryAcquire(key, 1).allowed();
    }

    @Override
    public double acquire(String key, int permits) throws InterruptedException {
        return AcquireSupport.acquire(clock, config, this, key, permits);
    }

    @Override
    public LimiterStats stats() {
        LimiterMetrics.Snapshot snapshot = metrics.snapshot();
        long minAvailable = Long.MAX_VALUE;
        for (RateLimiter limiter : limiters) {
            minAvailable = Math.min(minAvailable, limiter.stats().availablePermits());
        }
        return new LimiterStats(snapshot.allowedRequests(), snapshot.rejectedRequests(),
                snapshot.allowedPermits(), snapshot.rejectedPermits(),
                minAvailable == Long.MAX_VALUE ? 0 : minAvailable);
    }
}
