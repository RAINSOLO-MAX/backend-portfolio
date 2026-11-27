package dev.rainsolo.flowguard;

import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.limiter.SlidingWindowLimiter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlidingWindowAccuracyTest {

    @Test
    void expiredSlotsAreClearedSoTheTrailingWindowMoves() {
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(5).burstCapacity(5)
                .windowMillis(1_000).slidingSlots(10).build();
        MutableClock clock = new MutableClock(0);
        SlidingWindowLimiter limiter = new SlidingWindowLimiter(config, clock);

        assertEquals(5, drain(limiter, 6));
        clock.advanceMillis(999);
        assertFalse(limiter.tryAcquire("k", 1).allowed());
        clock.advanceMillis(1);
        assertTrue(limiter.tryAcquire("k", 1).allowed());
    }

    @Test
    void concurrentCallersNeverExceedTheWindowQuota() throws InterruptedException {
        int quota = 50;
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(quota).burstCapacity(quota)
                .windowMillis(1_000).slidingSlots(10).build();
        MutableClock clock = new MutableClock(0);
        SlidingWindowLimiter limiter = new SlidingWindowLimiter(config, clock);
        int threads = 16;
        int callsPerThread = 100;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger granted = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < callsPerThread; i++) {
                        if (limiter.tryAcquire("k", 1).allowed()) {
                            granted.incrementAndGet();
                        }
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals(quota, granted.get());
        assertEquals((long) (threads * callsPerThread - quota), limiter.stats().rejectedRequests());
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
