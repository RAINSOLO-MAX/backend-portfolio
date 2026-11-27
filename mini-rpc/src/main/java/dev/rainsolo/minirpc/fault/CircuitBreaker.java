package dev.rainsolo.minirpc.fault;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Count-based sliding-window circuit breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.
 * Only one probe is allowed in HALF_OPEN; a successful probe closes the circuit.
 */
public final class CircuitBreaker {

    public enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    private final int failureThreshold;
    private final int windowSize;
    private final long openDurationMillis;

    private final Deque<Boolean> window = new ArrayDeque<>();
    private State state = State.CLOSED;
    private long openedAt;
    private long totalRejected;

    public CircuitBreaker(int failureThreshold, int windowSize, long openDurationMillis) {
        this.failureThreshold = failureThreshold;
        this.windowSize = windowSize;
        this.openDurationMillis = openDurationMillis;
    }

    public synchronized boolean allowRequest() {
        if (state == State.OPEN) {
            if (System.currentTimeMillis() - openedAt >= openDurationMillis) {
                state = State.HALF_OPEN;
                return true;
            }
            totalRejected++;
            return false;
        }
        return true;
    }

    public synchronized void recordSuccess() {
        if (state == State.HALF_OPEN) {
            state = State.CLOSED;
            window.clear();
        }
        append(true);
    }

    public synchronized void recordFailure() {
        if (state == State.HALF_OPEN) {
            open();
            return;
        }
        append(false);
        if (state == State.CLOSED && failuresInWindow() >= failureThreshold) {
            open();
        }
    }

    private void open() {
        state = State.OPEN;
        openedAt = System.currentTimeMillis();
        window.clear();
    }

    private void append(boolean success) {
        window.addLast(success);
        while (window.size() > windowSize) {
            window.removeFirst();
        }
    }

    private long failuresInWindow() {
        return window.stream().filter(success -> !success).count();
    }

    public synchronized State state() {
        return state;
    }

    public synchronized long rejectedRequests() {
        return totalRejected;
    }
}
