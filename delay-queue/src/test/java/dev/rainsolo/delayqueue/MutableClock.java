package dev.rainsolo.delayqueue;

import dev.rainsolo.delayqueue.core.Clock;

/** Controllable clock: monotonic nanos advance explicitly, wall time can jump or rewind. */
public final class MutableClock implements Clock {

    private long nano;
    private long wall;

    public MutableClock(long initialWallMillis) {
        this.nano = System.nanoTime();
        this.wall = initialWallMillis;
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
    }

    public void setWall(long wallMillis) {
        this.wall = wallMillis;
    }
}
