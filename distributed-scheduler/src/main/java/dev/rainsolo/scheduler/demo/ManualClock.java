package dev.rainsolo.scheduler.demo;

import dev.rainsolo.scheduler.scheduler.Clock;

import java.util.concurrent.atomic.AtomicLong;

/** Deterministic clock for demos: nanoTime is derived from wall millis so heartbeats advance too. */
public final class ManualClock implements Clock {

    private final AtomicLong millis;

    public ManualClock(long startWallMillis) {
        this.millis = new AtomicLong(startWallMillis);
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
