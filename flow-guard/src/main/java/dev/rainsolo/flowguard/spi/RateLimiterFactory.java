package dev.rainsolo.flowguard.spi;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.RateLimiter;

/** SPI contract. Implementations are discovered through JDK ServiceLoader. */
public interface RateLimiterFactory {

    String algorithm();

    RateLimiter create(LimiterConfig config, Clock clock);
}
