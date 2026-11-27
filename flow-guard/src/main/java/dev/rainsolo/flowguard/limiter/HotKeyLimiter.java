package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.PermitResult;
import dev.rainsolo.flowguard.metrics.LimiterStats;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Decorator that keeps one independent limiter per key, evicting the least recently used. */
public final class HotKeyLimiter implements RateLimiter {

    private final Supplier<RateLimiter> factory;
    private final int maxTrackedKeys;
    private final LinkedHashMap<String, RateLimiter> limiters;

    public HotKeyLimiter(Supplier<RateLimiter> factory, int maxTrackedKeys) {
        this.factory = Objects.requireNonNull(factory, "factory");
        if (maxTrackedKeys <= 0) {
            throw new IllegalArgumentException("maxTrackedKeys must be positive");
        }
        this.maxTrackedKeys = maxTrackedKeys;
        final int max = maxTrackedKeys;
        this.limiters = new LinkedHashMap<>(max, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, RateLimiter> eldest) {
                return size() > max;
            }
        };
    }

    private RateLimiter limiterFor(String key) {
        synchronized (limiters) {
            RateLimiter existing = limiters.get(key);
            if (existing == null) {
                existing = factory.get();
                limiters.put(key, existing);
            }
            return existing;
        }
    }

    @Override
    public PermitResult tryAcquire(String key, int permits) {
        return limiterFor(key).tryAcquire(key, permits);
    }

    @Override
    public boolean tryAcquire(String key) {
        return limiterFor(key).tryAcquire(key);
    }

    @Override
    public double acquire(String key, int permits) throws InterruptedException {
        // Resolve the reference inside the lock, then block outside it so other keys proceed.
        return limiterFor(key).acquire(key, permits);
    }

    @Override
    public LimiterStats stats() {
        List<RateLimiter> snapshot;
        synchronized (limiters) {
            snapshot = new ArrayList<>(limiters.values());
        }
        long allowedRequests = 0;
        long rejectedRequests = 0;
        long allowedPermits = 0;
        long rejectedPermits = 0;
        long availablePermits = 0;
        for (RateLimiter limiter : snapshot) {
            LimiterStats stats = limiter.stats();
            allowedRequests += stats.allowedRequests();
            rejectedRequests += stats.rejectedRequests();
            allowedPermits += stats.allowedPermits();
            rejectedPermits += stats.rejectedPermits();
            availablePermits += stats.availablePermits();
        }
        return new LimiterStats(allowedRequests, rejectedRequests, allowedPermits,
                rejectedPermits, availablePermits);
    }

    public int trackedKeys() {
        synchronized (limiters) {
            return limiters.size();
        }
    }
}
