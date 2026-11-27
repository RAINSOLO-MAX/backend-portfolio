package dev.rainsolo.scheduler.scheduler;

import java.util.concurrent.atomic.AtomicLong;

/** Deterministic clock: nanoTime is derived from wall millis so heartbeat timeouts are testable. */
public final class MutableClock implements Clock {

    private final AtomicLong millis = new AtomicLong();

    public MutableClock() {
        this(0L);
    }

    public MutableClock(long startWallMillis) {
        this.millis.set(startWallMillis);
    }

    @Override
    public long wallTimeMillis() {
        return millis.get();
    }

    @Override
    public long nanoTime() {
        return millis.get() * 1_000_000L;
    }

    public void advanceMillis(long delta) {
        millis.addAndGet(delta);
    }

    public void setWallTimeMillis(long value) {
        millis.set(value);
    }
}
