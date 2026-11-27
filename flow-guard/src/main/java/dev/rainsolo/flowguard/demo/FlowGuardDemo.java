package dev.rainsolo.flowguard.demo;

import dev.rainsolo.flowguard.FlowGuard;
import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.SystemClock;
import dev.rainsolo.flowguard.limiter.FixedWindowCounterLimiter;
import dev.rainsolo.flowguard.limiter.HotKeyLimiter;
import dev.rainsolo.flowguard.limiter.LeakyBucketLimiter;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.spi.Limiters;

import java.util.List;

/** End-to-end demo: boundary burst, hot-key isolation, blocking acquire and the facade. */
public final class FlowGuardDemo {

    public static void main(String[] args) throws InterruptedException {
        boundaryScenario();
        hotKeyScenario();
        blockingScenario();
        facadeScenario();
    }

    private static void boundaryScenario() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(5).burstCapacity(5)
                .windowMillis(1_000).slidingSlots(10)
                .warmupMillis(0).build();
        System.out.println("---- boundary scenario: permitsPerSecond=5, 12 calls across the window edge ----");
        System.out.println("legend: A=allow R=reject | first 6 calls at t=990ms, last 6 at t=1010ms");
        for (String algorithm : List.of("fixed", "sliding", "leaky", "token")) {
            ManualClock clock = new ManualClock();
            RateLimiter limiter = Limiters.create(algorithm, config, clock);
            clock.advanceMillis(990);
            StringBuilder sequence = new StringBuilder();
            int allowed = 0;
            for (int i = 0; i < 6; i++) {
                if (limiter.tryAcquire("demo")) {
                    sequence.append('A');
                    allowed++;
                } else {
                    sequence.append('R');
                }
            }
            sequence.append('|');
            clock.advanceMillis(20);
            for (int i = 0; i < 6; i++) {
                if (limiter.tryAcquire("demo")) {
                    sequence.append('A');
                    allowed++;
                } else {
                    sequence.append('R');
                }
            }
            System.out.printf("%-8s %-13s allowed=%d%n", algorithm, sequence, allowed);
        }
        System.out.println("note: fixed resets at the edge -> 2x permits (critical burst); sliding stays smooth.");
    }

    private static void hotKeyScenario() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(2).burstCapacity(2)
                .windowMillis(1_000).build();
        ManualClock clock = new ManualClock();
        HotKeyLimiter hot = new HotKeyLimiter(
                () -> new FixedWindowCounterLimiter(config, clock), 10);
        System.out.println("\n---- hot-key isolation ----");
        System.out.println("user:A " + sequence(hot, "user:A"));
        System.out.println("user:B " + sequence(hot, "user:B"));
        System.out.println("tracked keys: " + hot.trackedKeys());
    }

    private static void blockingScenario() throws InterruptedException {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(50).burstCapacity(1)
                .windowMillis(1_000).build();
        RateLimiter blocking = new LeakyBucketLimiter(config, new SystemClock());
        blocking.tryAcquire("blocking", 1);
        double waited = blocking.acquire("blocking", 1);
        System.out.println("\n---- blocking acquire ----");
        System.out.printf("leaky bucket (rate=50/s, capacity=1) waited: %.3f s%n", waited);
    }

    private static void facadeScenario() {
        FlowGuard guard = FlowGuard.builder()
                .permitsPerSecond(3).burstCapacity(3)
                .algorithm("token").build();
        StringBuilder tenantOne = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            tenantOne.append(guard.tryAcquire("tenant-1").allowed() ? 'A' : 'R');
        }
        System.out.println("\n---- FlowGuard facade (one-line builder) ----");
        System.out.println("token/tenant-1: " + tenantOne);
        System.out.println("tenant-2 independent: " + (guard.tryAcquire("tenant-2").allowed() ? 'A' : 'R'));
        FlowGuard composed = FlowGuard.builder()
                .permitsPerSecond(3).burstCapacity(3)
                .rule("sliding").rule("leaky").perKey(false).build();
        System.out.println("composed rules first call: " + (composed.allow("shared") ? "A" : "R"));
        System.out.println("facade stats: " + guard.stats());
    }

    private static String sequence(RateLimiter limiter, String key) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            builder.append(limiter.tryAcquire(key) ? 'A' : 'R');
        }
        return builder.toString();
    }

    /** Deterministic clock so the demo can step across a window boundary without sleeping. */
    private static final class ManualClock implements Clock {

        private long nano;
        private long wall;

        @Override
        public long nanoTime() {
            return nano;
        }

        @Override
        public long wallTimeMillis() {
            return wall;
        }

        void advanceMillis(long millis) {
            nano += millis * 1_000_000L;
            wall += millis;
        }
    }
}
