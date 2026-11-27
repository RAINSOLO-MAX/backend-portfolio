package dev.rainsolo.minirpc.fault;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** One breaker per {@code serviceName@endpoint}, so a single bad node cannot open every circuit. */
public final class CircuitBreakers {

    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();
    private final int failureThreshold;
    private final int windowSize;
    private final long openDurationMillis;

    public CircuitBreakers(int failureThreshold, int windowSize, long openDurationMillis) {
        this.failureThreshold = failureThreshold;
        this.windowSize = windowSize;
        this.openDurationMillis = openDurationMillis;
    }

    public CircuitBreaker get(String key) {
        return breakers.computeIfAbsent(key, ignored ->
                new CircuitBreaker(failureThreshold, windowSize, openDurationMillis));
    }

    public int size() {
        return breakers.size();
    }
}
