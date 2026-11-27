package dev.rainsolo.flowguard.spi;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.LeakyBucketLimiter;
import dev.rainsolo.flowguard.limiter.RateLimiter;

/** SPI provider for the "leaky" algorithm. */
public final class LeakyBucketFactory implements RateLimiterFactory {

    @Override
    public String algorithm() {
        return "leaky";
    }

    @Override
    public RateLimiter create(LimiterConfig config, Clock clock) {
        return new LeakyBucketLimiter(config, clock);
    }
}
