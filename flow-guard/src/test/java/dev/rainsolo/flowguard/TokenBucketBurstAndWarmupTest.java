package dev.rainsolo.flowguard;

import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.TokenBucketLimiter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketBurstAndWarmupTest {

    @Test
    void aBurstNeverExceedsBurstCapacity() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(10).burstCapacity(5).build();
        MutableClock clock = new MutableClock(0);
        TokenBucketLimiter limiter = new TokenBucketLimiter(config, clock);
        assertFalse(limiter.tryAcquire("k", 6).allowed());
        assertEquals(5, drain(limiter, 10));
    }

    @Test
    void warmupRefillRateIsLowerThanTheSteadyRate() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(10).burstCapacity(100)
                .warmupMillis(1_000).build();
        MutableClock clock = new MutableClock(0);
        TokenBucketLimiter limiter = new TokenBucketLimiter(config, clock);
        drain(limiter, 100);

        clock.advanceMillis(1_000);
        int coldPhase = drain(limiter, 100);
        clock.advanceMillis(1_000);
        int steadyPhase = drain(limiter, 100);

        assertTrue(coldPhase > 0);
        assertTrue(coldPhase < steadyPhase, "warmup must throttle the early refill rate");
        assertEquals(10, steadyPhase);
    }

    @Test
    void aRewoundClockDoesNotMintExtraTokens() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(5).burstCapacity(100).build();
        MutableClock clock = new MutableClock(0);
        TokenBucketLimiter limiter = new TokenBucketLimiter(config, clock);
        drain(limiter, 100);
        clock.advanceMillis(1_000);
        // Larger than the refilled amount: triggers refill but consumes nothing.
        assertFalse(limiter.tryAcquire("k", 100).allowed());
        clock.rewindMillis(500);
        assertEquals(5, drain(limiter, 100));
    }

    private static int drain(TokenBucketLimiter limiter, int calls) {
        int count = 0;
        for (int i = 0; i < calls; i++) {
            if (limiter.tryAcquire("k", 1).allowed()) {
                count++;
            }
        }
        return count;
    }
}
