package dev.rainsolo.idemlock;

import dev.rainsolo.idemlock.core.Clock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Deterministic clock for tests: monotonic time can be advanced, wall time can be rewound. */
public final class MutableClock implements Clock {

    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);
    private final AtomicLong wall = new AtomicLong(1_700_000_000_000L);

    @Override
    public long nanoTime() {
        return nanos.get();
    }

    @Override
    public long wallTimeMillis() {
        return wall.get();
    }

    public void advanceMillis(long millis) {
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        wall.addAndGet(millis);
    }

    public void advanceNanos(long delta) {
        nanos.addAndGet(delta);
    }

    public void rewindWallClock(long millis) {
        wall.addAndGet(-millis);
    }

    public void setWallTimeMillis(long value) {
        wall.set(value);
    }
}
