package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;

/**
 * Water level leaks at a constant rate. Requests fill the bucket; anything above the
 * capacity is rejected and the wait is derived from the water level that must drain.
 */
public final class LeakyBucketLimiter extends AbstractRateLimiter {

    private final double capacity;
    private final double leakPerNano;
    private double water;
    private long lastNanos;

    public LeakyBucketLimiter(LimiterConfig config, Clock clock) {
        super(config, clock);
        this.capacity = config.burstCapacity();
        this.leakPerNano = config.permitsPerSecond() / 1_000_000_000.0;
        this.water = 0;
        this.lastNanos = clock.nanoTime();
    }

    @Override
    public synchronized PermitResult tryAcquire(String key, int permits) {
        checkPermits(permits);
        long now = clock.nanoTime();
        long delta = now - lastNanos;
        if (delta > 0) {
            water = Math.max(0.0, water - delta * leakPerNano);
            lastNanos = now;
        } else if (delta < 0) {
            // Clock moved backwards: do not leak, just rebase the reference point.
            lastNanos = now;
        }
        if (water + permits <= capacity) {
            water += permits;
            recordAllowed(permits);
            return PermitResult.permit((long) Math.max(0.0, capacity - water));
        }
        double overflow = water + permits - capacity;
        long waitMillis = (long) Math.ceil(overflow / config.permitsPerSecond() * 1000.0);
        recordRejected(permits);
        return PermitResult.reject(Math.max(1L, waitMillis));
    }

    @Override
    protected synchronized long availablePermits() {
        return (long) Math.max(0.0, capacity - water);
    }
}
