package dev.rainsolo.flowguard.spi;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.FixedWindowCounterLimiter;
import dev.rainsolo.flowguard.limiter.RateLimiter;

/** SPI provider for the "fixed" algorithm. */
public final class FixedWindowFactory implements RateLimiterFactory {

    @Override
    public String algorithm() {
        return "fixed";
    }

    @Override
    public RateLimiter create(LimiterConfig config, Clock clock) {
        return new FixedWindowCounterLimiter(config, clock);
    }
}
