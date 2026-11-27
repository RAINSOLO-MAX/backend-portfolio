package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.PermitResult;
import dev.rainsolo.flowguard.metrics.LimiterStats;

/** Contract for all rate limiting algorithms. Implementations must be thread safe. */
public interface RateLimiter {

    PermitResult tryAcquire(String key, int permits);

    boolean tryAcquire(String key);

    /** Blocks until the permits are granted; returns the wall time waited in seconds. */
    double acquire(String key, int permits) throws InterruptedException;

    LimiterStats stats();
}
