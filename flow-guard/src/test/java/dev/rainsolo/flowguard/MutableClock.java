package dev.rainsolo.flowguard;

import dev.rainsolo.flowguard.core.Clock;

/** Controllable clock: nanos advance or rewind explicitly, wall time is set directly. */
public final class MutableClock implements Clock {

    private long nano;
    private long wall;

    public MutableClock(long initialWallMillis) {
        this.nano = 0;
        this.wall = initialWallMillis;
    }

    public MutableClock() {
        this(0L);
    }

    @Override
    public long nanoTime() {
        return nano;
    }

    @Override
    public long wallTimeMillis() {
        return wall;
    }

    public void advanceMillis(long millis) {
        nano += millis * 1_000_000L;
        wall += millis;
    }

    public void advanceNanos(long nanos) {
        nano += nanos;
    }

    public void rewindMillis(long millis) {
        nano -= millis * 1_000_000L;
    }

    public void setNano(long value) {
        nano = value;
    }

    public void setWall(long value) {
        wall = value;
    }
}
