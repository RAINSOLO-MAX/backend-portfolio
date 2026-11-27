package dev.rainsolo.minirpc.config;

public final class RpcConfig {

    private final long requestTimeoutMillis;
    private final int maxAttempts;
    private final String loadBalance;
    private final int heartbeatIntervalSeconds;
    private final int serverReaderIdleSeconds;
    private final int failureThreshold;
    private final int windowSize;
    private final long circuitOpenMillis;

    private RpcConfig(Builder builder) {
        this.requestTimeoutMillis = builder.requestTimeoutMillis;
        this.maxAttempts = builder.maxAttempts;
        this.loadBalance = builder.loadBalance;
        this.heartbeatIntervalSeconds = builder.heartbeatIntervalSeconds;
        this.serverReaderIdleSeconds = builder.serverReaderIdleSeconds;
        this.failureThreshold = builder.failureThreshold;
        this.windowSize = builder.windowSize;
        this.circuitOpenMillis = builder.circuitOpenMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static RpcConfig defaults() {
        return builder().build();
    }

    public long requestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public String loadBalance() {
        return loadBalance;
    }

    public int heartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }

    public int serverReaderIdleSeconds() {
        return serverReaderIdleSeconds;
    }

    public int failureThreshold() {
        return failureThreshold;
    }

    public int windowSize() {
        return windowSize;
    }

    public long circuitOpenMillis() {
        return circuitOpenMillis;
    }

    public static final class Builder {
        private long requestTimeoutMillis = 3_000;
        private int maxAttempts = 3;
        private String loadBalance = "roundRobin";
        private int heartbeatIntervalSeconds = 15;
        private int serverReaderIdleSeconds = 60;
        private int failureThreshold = 5;
        private int windowSize = 20;
        private long circuitOpenMillis = 5_000;

        public Builder requestTimeoutMillis(long value) {
            this.requestTimeoutMillis = value;
            return this;
        }

        public Builder maxAttempts(int value) {
            this.maxAttempts = value;
            return this;
        }

        public Builder loadBalance(String value) {
            this.loadBalance = value;
            return this;
        }

        public Builder heartbeatIntervalSeconds(int value) {
            this.heartbeatIntervalSeconds = value;
            return this;
        }

        public Builder serverReaderIdleSeconds(int value) {
            this.serverReaderIdleSeconds = value;
            return this;
        }

        public Builder failureThreshold(int value) {
            this.failureThreshold = value;
            return this;
        }

        public Builder windowSize(int value) {
            this.windowSize = value;
            return this;
        }

        public Builder circuitOpenMillis(long value) {
            this.circuitOpenMillis = value;
            return this;
        }

        public RpcConfig build() {
            return new RpcConfig(this);
        }
    }
}
