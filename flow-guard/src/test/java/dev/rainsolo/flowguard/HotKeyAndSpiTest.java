package dev.rainsolo.flowguard;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.SystemClock;
import dev.rainsolo.flowguard.limiter.FixedWindowCounterLimiter;
import dev.rainsolo.flowguard.limiter.HotKeyLimiter;
import dev.rainsolo.flowguard.limiter.LeakyBucketLimiter;
import dev.rainsolo.flowguard.limiter.SlidingWindowLimiter;
import dev.rainsolo.flowguard.limiter.TokenBucketLimiter;
import dev.rainsolo.flowguard.spi.Limiters;
import dev.rainsolo.flowguard.spi.RateLimiterFactory;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HotKeyAndSpiTest {

    @Test
    void differentKeysHaveIndependentQuotas() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(1).burstCapacity(1).windowMillis(1_000).build();
        MutableClock clock = new MutableClock(0);
        HotKeyLimiter hot = new HotKeyLimiter(
                () -> new FixedWindowCounterLimiter(config, clock), 10);
        assertTrue(hot.tryAcquire("A", 1).allowed());
        assertFalse(hot.tryAcquire("A", 1).allowed());
        assertTrue(hot.tryAcquire("B", 1).allowed());
        assertEquals(2, hot.trackedKeys());
    }

    @Test
    void lruEvictionKeepsTrackedKeysWithinBound() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(1).burstCapacity(1).windowMillis(1_000)
                .maxTrackedKeys(2).build();
        MutableClock clock = new MutableClock(0);
        HotKeyLimiter hot = new HotKeyLimiter(
                () -> new FixedWindowCounterLimiter(config, clock), 2);
        assertTrue(hot.tryAcquire("a", 1).allowed());
        assertTrue(hot.tryAcquire("b", 1).allowed());
        assertTrue(hot.tryAcquire("c", 1).allowed());
        assertEquals(2, hot.trackedKeys());
        // "a" was evicted, so a fresh limiter grants the permit again.
        assertTrue(hot.tryAcquire("a", 1).allowed());
        assertEquals(2, hot.trackedKeys());
    }

    @Test
    void serviceLoaderDiscoversAllFourAlgorithms() {
        Set<String> names = new HashSet<>();
        for (RateLimiterFactory factory : ServiceLoader.load(RateLimiterFactory.class)) {
            names.add(factory.algorithm());
        }
        assertEquals(4, names.size());
        assertTrue(names.containsAll(Set.of("fixed", "sliding", "leaky", "token")));
        assertTrue(Limiters.availableAlgorithms().containsAll(names));
    }

    @Test
    void limitersCreatesEachAlgorithmByNameAndFallsBackForUnknownNames() {
        LimiterConfig config = LimiterConfig.defaults();
        Clock clock = new SystemClock();
        assertInstanceOf(FixedWindowCounterLimiter.class, Limiters.create("fixed", config, clock));
        assertInstanceOf(SlidingWindowLimiter.class, Limiters.create("sliding", config, clock));
        assertInstanceOf(LeakyBucketLimiter.class, Limiters.create("leaky", config, clock));
        assertInstanceOf(TokenBucketLimiter.class, Limiters.create("token", config, clock));
        assertInstanceOf(TokenBucketLimiter.class, Limiters.create("missing", config, clock));
    }
}
