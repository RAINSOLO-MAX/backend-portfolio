package dev.rainsolo.idemlock.lock;

import dev.rainsolo.idemlock.core.Clock;
import dev.rainsolo.idemlock.core.SystemClock;

import java.util.Objects;

/** Facade that builds locks sharing one store, clock, metrics and default configuration. */
public final class LockManager {

    private final LockStore store;
    private final Clock clock;
    private final LockMetrics metrics;
    private final long defaultLeaseMillis;
    private final boolean watchdogEnabled;

    private LockManager(Builder builder) {
        this.clock = builder.clock;
        this.store = builder.store != null ? builder.store : new InMemoryLockStore(builder.clock);
        this.metrics = builder.metrics;
        this.defaultLeaseMillis = builder.defaultLeaseMillis;
        this.watchdogEnabled = builder.watchdogEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static LockManager defaults() {
        return builder().build();
    }

    /** Creates a lock instance bound to the calling thread; call tryLock to actually acquire it. */
    public DistributedLock lock(String name) {
        return new ReentrantDistributedLock(name, store, clock, metrics, watchdogEnabled);
    }

    /** Tries once with the configured default lease; returns {@code null} when held by someone else. */
    public DistributedLock acquire(String name) {
        DistributedLock lock = lock(name);
        return lock.tryLock(defaultLeaseMillis) ? lock : null;
    }

    public LockMetrics metrics() {
        return metrics;
    }

    public LockStore store() {
        return store;
    }

    public Clock clock() {
        return clock;
    }

    public long defaultLeaseMillis() {
        return defaultLeaseMillis;
    }

    public boolean watchdogEnabled() {
        return watchdogEnabled;
    }

    /** Fluent builder; a custom clock is also applied to the default in-memory store. */
    public static final class Builder {

        private Clock clock = new SystemClock();
        private LockStore store;
        private LockMetrics metrics = new LockMetrics();
        private long defaultLeaseMillis = 30_000;
        private boolean watchdogEnabled = true;

        public Builder clock(Clock value) {
            this.clock = Objects.requireNonNull(value, "clock");
            return this;
        }

        public Builder store(LockStore value) {
            this.store = Objects.requireNonNull(value, "store");
            return this;
        }

        public Builder metrics(LockMetrics value) {
            this.metrics = Objects.requireNonNull(value, "metrics");
            return this;
        }

        public Builder defaultLeaseMillis(long value) {
            if (value <= 0) {
                throw new IllegalArgumentException("defaultLeaseMillis must be positive");
            }
            this.defaultLeaseMillis = value;
            return this;
        }

        public Builder watchdog(boolean value) {
            this.watchdogEnabled = value;
            return this;
        }

        public LockManager build() {
            return new LockManager(this);
        }
    }
}
