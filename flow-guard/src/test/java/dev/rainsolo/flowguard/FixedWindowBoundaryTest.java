package dev.rainsolo.flowguard;

import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.FixedWindowCounterLimiter;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.limiter.SlidingWindowLimiter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedWindowBoundaryTest {

    private static LimiterConfig config() {
        return LimiterConfig.builder()
                .permitsPerSecond(5).burstCapacity(5)
                .windowMillis(1_000).slidingSlots(10)
                .build();
    }

    @Test
    void fixedWindowAllowsTwiceThePermitsAcrossTheBoundary() {
        MutableClock clock = new MutableClock(0);
        RateLimiter limiter = new FixedWindowCounterLimiter(config(), clock);
        clock.advanceMillis(990);
        int beforeEdge = allowed(limiter, 6);
        clock.advanceMillis(20);
        int afterEdge = allowed(limiter, 6);

        assertEquals(5, beforeEdge);
        assertEquals(5, afterEdge);
        assertEquals(10, beforeEdge + afterEdge);
        assertEquals(2 * config().windowPermits(), beforeEdge + afterEdge);
    }

    @Test
    void slidingWindowDoesNotBurstAcrossTheBoundary() {
        MutableClock clock = new MutableClock(0);
        RateLimiter limiter = new SlidingWindowLimiter(config(), clock);
        clock.advanceMillis(990);
        int beforeEdge = allowed(limiter, 6);
        clock.advanceMillis(20);
        int afterEdge = allowed(limiter, 6);

        assertEquals(5, beforeEdge);
        assertTrue(beforeEdge + afterEdge < 10,
                "sliding window must not exhibit the 2x critical burst");
        assertEquals(5, beforeEdge + afterEdge);
    }

    private static int allowed(RateLimiter limiter, int calls) {
        int count = 0;
        for (int i = 0; i < calls; i++) {
            if (limiter.tryAcquire("k", 1).allowed()) {
                count++;
            }
        }
        return count;
    }
}
