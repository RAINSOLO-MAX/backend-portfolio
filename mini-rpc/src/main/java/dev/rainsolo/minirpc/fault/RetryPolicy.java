package dev.rainsolo.minirpc.fault;

import dev.rainsolo.minirpc.common.RpcException;

import java.util.concurrent.ThreadLocalRandom;

/** Retries transport failures only, with exponential backoff and full jitter. */
public final class RetryPolicy {

    private final int maxAttempts;
    private final long baseBackoffMillis;
    private final double multiplier;
    private final long maxBackoffMillis;

    public RetryPolicy(int maxAttempts, long baseBackoffMillis, double multiplier, long maxBackoffMillis) {
        this.maxAttempts = Math.max(1, maxAttempts);
        this.baseBackoffMillis = baseBackoffMillis;
        this.multiplier = multiplier;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    public boolean shouldRetry(RpcException failure, int attemptsUsed) {
        return failure.isRetryable() && attemptsUsed < maxAttempts;
    }

    public long backoffMillis(int attemptsUsed) {
        double raw = baseBackoffMillis * Math.pow(multiplier, Math.max(0, attemptsUsed - 1));
        long capped = (long) Math.min(raw, maxBackoffMillis);
        return capped <= 0 ? 0 : ThreadLocalRandom.current().nextLong(capped + 1);
    }

    public int maxAttempts() {
        return maxAttempts;
    }
}
