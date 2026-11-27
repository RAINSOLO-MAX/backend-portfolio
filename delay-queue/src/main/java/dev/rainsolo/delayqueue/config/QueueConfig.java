package dev.rainsolo.delayqueue.config;

public final class QueueConfig {

    private final long tickMillis;
    private final int l0Slots;
    private final long l1TickMillis;
    private final int l1Slots;
    private final long l2TickMillis;
    private final int l2Slots;
    private final int defaultMaxAttempts;
    private final long baseBackoffMillis;
    private final double backoffFactor;
    private final long maxBackoffMillis;

    private QueueConfig(Builder builder) {
        this.tickMillis = builder.tickMillis;
        this.l0Slots = builder.l0Slots;
        this.l1TickMillis = builder.l1TickMillis;
        this.l1Slots = builder.l1Slots;
        this.l2TickMillis = builder.l2TickMillis;
        this.l2Slots = builder.l2Slots;
        this.defaultMaxAttempts = builder.defaultMaxAttempts;
        this.baseBackoffMillis = builder.baseBackoffMillis;
        this.backoffFactor = builder.backoffFactor;
        this.maxBackoffMillis = builder.maxBackoffMillis;
        if (tickMillis <= 0 || l1TickMillis % tickMillis != 0 || l2TickMillis % l1TickMillis != 0) {
            throw new IllegalArgumentException("Wheel tick durations must be exact multiples of the lower level");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static QueueConfig defaults() {
        return builder().build();
    }

    public long tickMillis() {
        return tickMillis;
    }

    public int l0Slots() {
        return l0Slots;
    }

    public long l1TickMillis() {
        return l1TickMillis;
    }

    public int l1Slots() {
        return l1Slots;
    }

    public long l2TickMillis() {
        return l2TickMillis;
    }

    public int l2Slots() {
        return l2Slots;
    }

    public int l1Ratio() {
        return (int) (l1TickMillis / tickMillis);
    }

    public int l2Ratio() {
        return (int) (l2TickMillis / l1TickMillis);
    }

    public int defaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public long baseBackoffMillis() {
        return baseBackoffMillis;
    }

    public double backoffFactor() {
        return backoffFactor;
    }

    public long maxBackoffMillis() {
        return maxBackoffMillis;
    }

    public static final class Builder {
        private long tickMillis = 10;
        private int l0Slots = 512;
        private long l1TickMillis = 100;
        private int l1Slots = 600;
        private long l2TickMillis = 1_000;
        private int l2Slots = 3_600;
        private int defaultMaxAttempts = 3;
        private long baseBackoffMillis = 50;
        private double backoffFactor = 2.0;
        private long maxBackoffMillis = 5_000;

        public Builder tickMillis(long value) {
            this.tickMillis = value;
            return this;
        }

        public Builder l0Slots(int value) {
            this.l0Slots = value;
            return this;
        }

        public Builder l1TickMillis(long value) {
            this.l1TickMillis = value;
            return this;
        }

        public Builder l1Slots(int value) {
            this.l1Slots = value;
            return this;
        }

        public Builder l2TickMillis(long value) {
            this.l2TickMillis = value;
            return this;
        }

        public Builder l2Slots(int value) {
            this.l2Slots = value;
            return this;
        }

        public Builder defaultMaxAttempts(int value) {
            this.defaultMaxAttempts = value;
            return this;
        }

        public Builder baseBackoffMillis(long value) {
            this.baseBackoffMillis = value;
            return this;
        }

        public Builder backoffFactor(double value) {
            this.backoffFactor = value;
            return this;
        }

        public Builder maxBackoffMillis(long value) {
            this.maxBackoffMillis = value;
            return this;
        }

        public QueueConfig build() {
            return new QueueConfig(this);
        }
    }
}
