package dev.rainsolo.flowguard.core;

/** Raised when a blocking acquire cannot obtain permits within the configured timeout. */
public class RateLimitExceededException extends RuntimeException {

    private final String key;
    private final long retryAfterMillis;

    public RateLimitExceededException(String key, long retryAfterMillis) {
        super("rate limit exceeded for key='" + key + "', retry after " + retryAfterMillis + " ms");
        this.key = key;
        this.retryAfterMillis = retryAfterMillis;
    }

    public String key() {
        return key;
    }

    public long retryAfterMillis() {
        return retryAfterMillis;
    }
}
