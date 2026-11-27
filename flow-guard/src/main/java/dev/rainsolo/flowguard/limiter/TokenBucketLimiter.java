package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;

import java.util.concurrent.TimeUnit;

/**
 * Tokens refill continuously from nanoTime and are capped at burstCapacity, so a full
 * bucket grants a real burst. During warmup the refill rate rises linearly from a cold
 * value up to permitsPerSecond. A non positive time delta never mints tokens.
 */
public final class TokenBucketLimiter extends AbstractRateLimiter {

    private static final double COLD_FACTOR = 3.0;

    private final double capacity;
    private final long warmupNanos;
    private final long startNanos;
    private double tokens;
    private long lastRefillNanos;

    public TokenBucketLimiter(LimiterConfig config, Clock clock) {
        super(config, clock);
        this.capacity = config.burstCapacity();
        this.warmupNanos = TimeUnit.MILLISECONDS.toNanos(config.warmupMillis());
        this.startNanos = clock.nanoTime();
        this.lastRefillNanos = startNanos;
        this.tokens = capacity;
    }

    private double rateAt(long tNanos) {
        if (warmupNanos <= 0) {
            return permitsPerSecond;
        }
        long elapsed = Math.max(0L, tNanos - startNanos);
        if (elapsed >= warmupNanos) {
            return permitsPerSecond;
        }
        double cold = permitsPerSecond / COLD_FACTOR;
        return cold + (permitsPerSecond - cold) * ((double) elapsed / warmupNanos);
    }

    private double integrate(long from, long to) {
        if (to <= from) {
            return 0.0;
        }
        double averageRate = (rateAt(from) + rateAt(to)) / 2.0;
        return averageRate * ((to - from) / 1_000_000_000.0);
    }

    private double producedBetween(long from, long to) {
        if (warmupNanos <= 0) {
            return permitsPerSecond * ((to - from) / 1_000_000_000.0);
        }
        long warmEnd = startNanos + warmupNanos;
        if (from >= warmEnd) {
            return permitsPerSecond * ((to - from) / 1_000_000_000.0);
        }
        if (to <= warmEnd) {
            return integrate(from, to);
        }
        return integrate(from, warmEnd)
                + permitsPerSecond * ((to - warmEnd) / 1_000_000_000.0);
    }

    private void refill(long now) {
        if (now <= lastRefillNanos) {
            // Backwards or stalled clock: no extra tokens, rebase to avoid double counting.
            lastRefillNanos = now;
            return;
        }
        tokens = Math.min(capacity, tokens + producedBetween(lastRefillNanos, now));
        lastRefillNanos = now;
    }

    @Override
    public synchronized PermitResult tryAcquire(String key, int permits) {
        checkPermits(permits);
        long now = clock.nanoTime();
        refill(now);
        if (tokens >= permits) {
            tokens -= permits;
            recordAllowed(permits);
            return PermitResult.permit((long) tokens);
        }
        double deficit = permits - tokens;
        double rate = Math.max(1e-9, rateAt(now));
        long waitMillis = (long) Math.ceil(deficit / rate * 1000.0);
        recordRejected(permits);
        return PermitResult.reject(Math.max(1L, waitMillis));
    }

    @Override
    protected synchronized long availablePermits() {
        return (long) Math.max(0.0, tokens);
    }
}
