package dev.rainsolo.flowguard.spi;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.limiter.TokenBucketLimiter;

/** SPI provider for the "token" algorithm. */
public final class TokenBucketFactory implements RateLimiterFactory {

    @Override
    public String algorithm() {
        return "token";
    }

    @Override
    public RateLimiter create(LimiterConfig config, Clock clock) {
        return new TokenBucketLimiter(config, clock);
    }
}
