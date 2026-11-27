package dev.rainsolo.idemlock.core;

/**
 * Configuration of an idempotent executor.
 *
 * @param ttlMillis          how long a terminal result stays cached
 * @param waitForResult      whether concurrent callers wait for the in-flight result
 * @param waitTimeoutMillis  maximum time a concurrent caller waits, in milliseconds
 * @param maxAttempts        maximum number of execution attempts for one key
 */
public record IdempotentConfig(long ttlMillis, boolean waitForResult, long waitTimeoutMillis, int maxAttempts) {

    public IdempotentConfig {
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be positive");
        }
        if (waitTimeoutMillis < 0) {
            throw new IllegalArgumentException("waitTimeoutMillis must not be negative");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IdempotentConfig defaults() {
        return builder().build();
    }

    /** Fluent builder with production-friendly defaults. */
    public static final class Builder {

        private long ttlMillis = 60_000;
        private boolean waitForResult = true;
        private long waitTimeoutMillis = 5_000;
        private int maxAttempts = 3;

        public Builder ttlMillis(long value) {
            this.ttlMillis = value;
            return this;
        }

        public Builder waitForResult(boolean value) {
            this.waitForResult = value;
            return this;
        }

        public Builder waitTimeoutMillis(long value) {
            this.waitTimeoutMillis = value;
            return this;
        }

        public Builder maxAttempts(int value) {
            this.maxAttempts = value;
            return this;
        }

        public IdempotentConfig build() {
            return new IdempotentConfig(ttlMillis, waitForResult, waitTimeoutMillis, maxAttempts);
        }
    }
}
