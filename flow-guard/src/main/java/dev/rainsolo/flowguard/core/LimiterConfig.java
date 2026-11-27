package dev.rainsolo.flowguard.core;

/** Immutable limiter configuration built through a fluent builder. */
public final class LimiterConfig {

    private final double permitsPerSecond;
    private final long burstCapacity;
    private final long windowMillis;
    private final int slidingSlots;
    private final long warmupMillis;
    private final int maxTrackedKeys;
    private final long waitTimeoutMillis;

    private LimiterConfig(Builder builder) {
        this.permitsPerSecond = builder.permitsPerSecond;
        this.burstCapacity = builder.burstCapacity;
        this.windowMillis = builder.windowMillis;
        this.slidingSlots = builder.slidingSlots;
        this.warmupMillis = builder.warmupMillis;
        this.maxTrackedKeys = builder.maxTrackedKeys;
        this.waitTimeoutMillis = builder.waitTimeoutMillis;
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (burstCapacity <= 0) {
            throw new IllegalArgumentException("burstCapacity must be positive");
        }
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be positive");
        }
        if (slidingSlots <= 0) {
            throw new IllegalArgumentException("slidingSlots must be positive");
        }
        if (warmupMillis < 0) {
            throw new IllegalArgumentException("warmupMillis must not be negative");
        }
        if (maxTrackedKeys <= 0) {
            throw new IllegalArgumentException("maxTrackedKeys must be positive");
        }
        if (waitTimeoutMillis < 0) {
            throw new IllegalArgumentException("waitTimeoutMillis must not be negative");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static LimiterConfig defaults() {
        return builder().build();
    }

    public double permitsPerSecond() {
        return permitsPerSecond;
    }

    public long burstCapacity() {
        return burstCapacity;
    }

    public long windowMillis() {
        return windowMillis;
    }

    public int slidingSlots() {
        return slidingSlots;
    }

    public long warmupMillis() {
        return warmupMillis;
    }

    public int maxTrackedKeys() {
        return maxTrackedKeys;
    }

    /** 0 means a blocking acquire waits indefinitely. */
    public long waitTimeoutMillis() {
        return waitTimeoutMillis;
    }

    /** Permits allowed inside one full window, used by the counter based limiters. */
    public long windowPermits() {
        return Math.max(1L, Math.round(permitsPerSecond * windowMillis / 1000.0));
    }

    public Builder toBuilder() {
        return builder()
                .permitsPerSecond(permitsPerSecond)
                .burstCapacity(burstCapacity)
                .windowMillis(windowMillis)
                .slidingSlots(slidingSlots)
                .warmupMillis(warmupMillis)
                .maxTrackedKeys(maxTrackedKeys)
                .waitTimeoutMillis(waitTimeoutMillis);
    }

    public static final class Builder {
        private double permitsPerSecond = 10;
        private long burstCapacity = 10;
        private long windowMillis = 1_000;
        private int slidingSlots = 10;
        private long warmupMillis = 0;
        private int maxTrackedKeys = 10_000;
        private long waitTimeoutMillis = 0;

        public Builder permitsPerSecond(double value) {
            this.permitsPerSecond = value;
            return this;
        }

        public Builder burstCapacity(long value) {
            this.burstCapacity = value;
            return this;
        }

        public Builder windowMillis(long value) {
            this.windowMillis = value;
            return this;
        }

        public Builder slidingSlots(int value) {
            this.slidingSlots = value;
            return this;
        }

        public Builder warmupMillis(long value) {
            this.warmupMillis = value;
            return this;
        }

        public Builder maxTrackedKeys(int value) {
            this.maxTrackedKeys = value;
            return this;
        }

        public Builder waitTimeoutMillis(long value) {
            this.waitTimeoutMillis = value;
            return this;
        }

        public LimiterConfig build() {
            return new LimiterConfig(this);
        }
    }
}
