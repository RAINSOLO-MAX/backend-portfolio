package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;
import dev.rainsolo.flowguard.core.RateLimitExceededException;

import java.util.concurrent.TimeUnit;

/** Shared blocking loop: retry on rejection, sleep for the advertised wait, honour the timeout. */
final class AcquireSupport {

    private AcquireSupport() {
    }

    static double acquire(Clock clock, LimiterConfig config, RateLimiter target,
                          String key, int permits) throws InterruptedException {
        long startNanos = clock.nanoTime();
        long timeoutMillis = config.waitTimeoutMillis();
        long deadlineNanos = timeoutMillis > 0
                ? startNanos + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
                : Long.MAX_VALUE;
        long lastWaitMillis;
        while (true) {
            PermitResult result = target.tryAcquire(key, permits);
            if (result.allowed()) {
                break;
            }
            lastWaitMillis = Math.max(1L, result.waitMillis());
            long nowNanos = clock.nanoTime();
            if (nowNanos >= deadlineNanos) {
                throw new RateLimitExceededException(key, lastWaitMillis);
            }
            long remainingMillis = Math.max(1L,
                    TimeUnit.NANOSECONDS.toMillis(deadlineNanos - nowNanos));
            Thread.sleep(Math.min(lastWaitMillis, remainingMillis));
        }
        return (clock.nanoTime() - startNanos) / 1_000_000_000.0;
    }
}
