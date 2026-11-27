package dev.rainsolo.flowguard;

import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;
import dev.rainsolo.flowguard.core.SystemClock;
import dev.rainsolo.flowguard.limiter.FixedWindowCounterLimiter;
import dev.rainsolo.flowguard.limiter.LeakyBucketLimiter;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.limiter.RuleChain;
import dev.rainsolo.flowguard.limiter.TokenBucketLimiter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeakyAndRuleChainTest {

    @Test
    void leakyBucketLeaksAtAConstantRateAndRejectsOverCapacity() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(10).burstCapacity(5)
                .windowMillis(1_000).build();
        MutableClock clock = new MutableClock(0);
        LeakyBucketLimiter limiter = new LeakyBucketLimiter(config, clock);

        assertEquals(5, drain(limiter, 7));
        clock.advanceMillis(100);
        assertEquals(1, drain(limiter, 7));
        clock.advanceMillis(200);
        assertEquals(2, drain(limiter, 7));
        assertFalse(limiter.tryAcquire("k", 6).allowed());
    }

    @Test
    void ruleChainRejectsWhenAnyRuleRejects() {
        LimiterConfig strict = LimiterConfig.builder()
                .permitsPerSecond(1).burstCapacity(1).windowMillis(1_000).build();
        LimiterConfig loose = LimiterConfig.builder()
                .permitsPerSecond(1_000).burstCapacity(1_000).windowMillis(1_000).build();
        MutableClock clock = new MutableClock(0);
        FixedWindowCounterLimiter exhausted = new FixedWindowCounterLimiter(strict, clock);
        assertTrue(exhausted.tryAcquire("k", 1).allowed());
        TokenBucketLimiter generous = new TokenBucketLimiter(loose, clock);

        RuleChain chain = new RuleChain(clock, strict, exhausted, generous);
        PermitResult result = chain.tryAcquire("k", 1);
        assertFalse(result.allowed());
        assertTrue(result.waitMillis() > 0);
    }

    @Test
    void ruleChainReportsTheStrictestRetryAfter() {
        MutableClock clock = new MutableClock(0);
        clock.advanceMillis(500);
        LimiterConfig shortWindow = LimiterConfig.builder()
                .permitsPerSecond(10).burstCapacity(10).windowMillis(1_000).build();
        LimiterConfig longWindow = LimiterConfig.builder()
                .permitsPerSecond(10).burstCapacity(10).windowMillis(4_000).build();
        FixedWindowCounterLimiter a = new FixedWindowCounterLimiter(shortWindow, clock);
        FixedWindowCounterLimiter b = new FixedWindowCounterLimiter(longWindow, clock);
        int drained = 0;
        while (a.tryAcquire("k", 1).allowed()) {
            drained++;
        }
        while (b.tryAcquire("k", 1).allowed()) {
            drained++;
        }
        assertTrue(drained > 0, "both counters must be exhausted first");

        RuleChain chain = new RuleChain(clock, shortWindow, a, b);
        PermitResult result = chain.tryAcquire("k", 1);
        assertFalse(result.allowed());
        // max(wait of the 1s window, wait of the 4s window) = max(500, 3500)
        assertEquals(3_500, result.waitMillis());
    }

    @Test
    void ruleChainAllowsOnlyWhenEveryRuleAllows() {
        MutableClock clock = new MutableClock(0);
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(5).burstCapacity(5).windowMillis(1_000).build();
        RuleChain chain = new RuleChain(clock, config,
                new FixedWindowCounterLimiter(config, clock),
                new TokenBucketLimiter(config, clock));
        PermitResult result = chain.tryAcquire("k", 1);
        assertTrue(result.allowed());
        assertEquals(4, result.remainingPermits());
    }

    @Test
    void blockingAcquireReturnsAReasonableWait() throws InterruptedException {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(50).burstCapacity(1).windowMillis(1_000).build();
        LeakyBucketLimiter limiter = new LeakyBucketLimiter(config, new SystemClock());
        assertTrue(limiter.tryAcquire("k", 1).allowed());
        double waitedSeconds = limiter.acquire("k", 1);
        assertTrue(waitedSeconds > 0.0, "blocking acquire must actually wait");
        assertTrue(waitedSeconds < 2.0, "wait should stay bounded for a small bucket");
    }

    private static int drain(RateLimiter limiter, int calls) {
        int count = 0;
        for (int i = 0; i < calls; i++) {
            if (limiter.tryAcquire("k", 1).allowed()) {
                count++;
            }
        }
        return count;
    }
}
