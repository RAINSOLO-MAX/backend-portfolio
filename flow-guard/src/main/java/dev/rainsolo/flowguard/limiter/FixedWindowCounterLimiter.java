package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts permits per fixed window and resets exactly at the boundary. The reset is what
 * allows a 2x critical burst when traffic lands on both sides of a boundary.
 */
public final class FixedWindowCounterLimiter extends AbstractRateLimiter {

    private final long windowMillis;
    private final long windowPermits;
    private final AtomicLong counter = new AtomicLong();
    private long windowStartMillis = Long.MIN_VALUE;

    public FixedWindowCounterLimiter(LimiterConfig config, Clock clock) {
        super(config, clock);
        this.windowMillis = config.windowMillis();
        this.windowPermits = config.windowPermits();
    }

    @Override
    public synchronized PermitResult tryAcquire(String key, int permits) {
        checkPermits(permits);
        long nowMillis = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime());
        long boundary = (nowMillis / windowMillis) * windowMillis;
        if (boundary != windowStartMillis) {
            counter.set(0);
            windowStartMillis = boundary;
        }
        long used = counter.get();
        if (used + permits <= windowPermits) {
            counter.addAndGet(permits);
            recordAllowed(permits);
            return PermitResult.permit(windowPermits - counter.get());
        }
        recordRejected(permits);
        long waitMillis = Math.max(1L, windowStartMillis + windowMillis - nowMillis);
        return PermitResult.reject(waitMillis);
    }

    @Override
    protected synchronized long availablePermits() {
        return Math.max(0L, windowPermits - counter.get());
    }
}
