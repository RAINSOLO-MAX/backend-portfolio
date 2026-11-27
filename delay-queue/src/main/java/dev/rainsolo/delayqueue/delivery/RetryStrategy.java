package dev.rainsolo.delayqueue.delivery;

import java.util.concurrent.ThreadLocalRandom;

/** Exponential backoff with equal jitter: half fixed delay plus a random half. */
public final class RetryStrategy {

    private RetryStrategy() {
    }

    public static long backoffMillis(int attempt, long baseMillis, double factor, long maxMillis) {
        double raw = baseMillis * Math.pow(factor, Math.max(0, attempt - 1));
        long capped = (long) Math.min(raw, maxMillis);
        long half = Math.max(1, capped / 2);
        return half + ThreadLocalRandom.current().nextLong(half + 1);
    }
}
