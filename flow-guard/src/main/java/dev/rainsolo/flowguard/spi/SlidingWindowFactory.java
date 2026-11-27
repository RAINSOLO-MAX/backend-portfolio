package dev.rainsolo.flowguard.spi;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.limiter.SlidingWindowLimiter;

/** SPI provider for the "sliding" algorithm. */
public final class SlidingWindowFactory implements RateLimiterFactory {

    @Override
    public String algorithm() {
        return "sliding";
    }

    @Override
    public RateLimiter create(LimiterConfig config, Clock clock) {
        return new SlidingWindowLimiter(config, clock);
    }
}
