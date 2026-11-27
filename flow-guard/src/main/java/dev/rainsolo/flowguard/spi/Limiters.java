package dev.rainsolo.flowguard.spi;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.limiter.FixedWindowCounterLimiter;
import dev.rainsolo.flowguard.limiter.LeakyBucketLimiter;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.limiter.SlidingWindowLimiter;
import dev.rainsolo.flowguard.limiter.TokenBucketLimiter;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/** Facade that resolves algorithms by name through SPI, falling back to built-in instances. */
public final class Limiters {

    private static final Map<String, RateLimiterFactory> FACTORIES = load();

    private Limiters() {
    }

    private static Map<String, RateLimiterFactory> load() {
        Map<String, RateLimiterFactory> providers = new HashMap<>();
        for (RateLimiterFactory factory : ServiceLoader.load(RateLimiterFactory.class)) {
            providers.putIfAbsent(factory.algorithm(), factory);
        }
        return Map.copyOf(providers);
    }

    public static RateLimiter create(String algorithm, LimiterConfig config, Clock clock) {
        RateLimiterFactory factory = FACTORIES.get(algorithm);
        if (factory != null) {
            return factory.create(config, clock);
        }
        return builtIn(algorithm, config, clock);
    }

    private static RateLimiter builtIn(String algorithm, LimiterConfig config, Clock clock) {
        String name = algorithm == null ? "" : algorithm;
        return switch (name) {
            case "fixed" -> new FixedWindowCounterLimiter(config, clock);
            case "sliding" -> new SlidingWindowLimiter(config, clock);
            case "leaky" -> new LeakyBucketLimiter(config, clock);
            case "token" -> new TokenBucketLimiter(config, clock);
            default -> new TokenBucketLimiter(config, clock);
        };
    }

    /** SPI names plus the four built-in names, so callers always see a complete set. */
    public static Set<String> availableAlgorithms() {
        Set<String> names = new LinkedHashSet<>(FACTORIES.keySet());
        names.addAll(List.of("fixed", "sliding", "leaky", "token"));
        return names;
    }
}
