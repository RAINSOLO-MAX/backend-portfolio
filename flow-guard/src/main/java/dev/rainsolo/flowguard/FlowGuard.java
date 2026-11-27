package dev.rainsolo.flowguard;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;
import dev.rainsolo.flowguard.core.SystemClock;
import dev.rainsolo.flowguard.limiter.HotKeyLimiter;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.limiter.RuleChain;
import dev.rainsolo.flowguard.metrics.LimiterStats;
import dev.rainsolo.flowguard.spi.Limiters;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** One-line facade: configure algorithms, optionally compose rules, then chain tryAcquire calls. */
public final class FlowGuard {

    private final RateLimiter limiter;
    private final LimiterConfig config;

    private FlowGuard(RateLimiter limiter, LimiterConfig config) {
        this.limiter = limiter;
        this.config = config;
    }

    public static Builder builder() {
        return new Builder();
    }

    public PermitResult tryAcquire(String key) {
        return limiter.tryAcquire(key, 1);
    }

    public PermitResult tryAcquire(String key, int permits) {
        return limiter.tryAcquire(key, permits);
    }

    public boolean allow(String key) {
        return limiter.tryAcquire(key);
    }

    public double acquire(String key, int permits) throws InterruptedException {
        return limiter.acquire(key, permits);
    }

    public LimiterStats stats() {
        return limiter.stats();
    }

    public LimiterConfig config() {
        return config;
    }

    public static final class Builder {
        private LimiterConfig.Builder delegate = LimiterConfig.builder();
        private Clock clock = new SystemClock();
        private final List<String> algorithms = new ArrayList<>();
        private boolean perKey = true;

        public Builder config(LimiterConfig config) {
            this.delegate = Objects.requireNonNull(config, "config").toBuilder();
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public Builder permitsPerSecond(double value) {
            delegate.permitsPerSecond(value);
            return this;
        }

        public Builder burstCapacity(long value) {
            delegate.burstCapacity(value);
            return this;
        }

        public Builder windowMillis(long value) {
            delegate.windowMillis(value);
            return this;
        }

        public Builder slidingSlots(int value) {
            delegate.slidingSlots(value);
            return this;
        }

        public Builder warmupMillis(long value) {
            delegate.warmupMillis(value);
            return this;
        }

        public Builder maxTrackedKeys(int value) {
            delegate.maxTrackedKeys(value);
            return this;
        }

        public Builder waitTimeoutMillis(long value) {
            delegate.waitTimeoutMillis(value);
            return this;
        }

        /** Sets the primary algorithm, replacing any previously added rules. */
        public Builder algorithm(String name) {
            algorithms.clear();
            algorithms.add(name);
            return this;
        }

        /** Adds a rule; multiple rules are combined with AND semantics. */
        public Builder rule(String name) {
            algorithms.add(name);
            return this;
        }

        /** Whether to keep an independent limiter chain per key (default true). */
        public Builder perKey(boolean value) {
            this.perKey = value;
            return this;
        }

        public FlowGuard build() {
            final LimiterConfig cfg = delegate.build();
            final Clock clk = clock;
            final List<String> names = algorithms.isEmpty()
                    ? List.of("token") : List.copyOf(algorithms);
            Supplier<RateLimiter> chainFactory = () -> {
                List<RateLimiter> rules = new ArrayList<>();
                for (String name : names) {
                    rules.add(Limiters.create(name, cfg, clk));
                }
                return rules.size() == 1 ? rules.get(0) : new RuleChain(clk, cfg, rules);
            };
            RateLimiter root = perKey
                    ? new HotKeyLimiter(chainFactory, cfg.maxTrackedKeys())
                    : chainFactory.get();
            return new FlowGuard(root, cfg);
        }
    }
}
