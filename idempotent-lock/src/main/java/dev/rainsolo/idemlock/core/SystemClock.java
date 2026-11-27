package dev.rainsolo.idemlock.core;

/** Clock backed by the JVM monotonic clock and the system wall clock. */
public final class SystemClock implements Clock {

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public long wallTimeMillis() {
        return System.currentTimeMillis();
    }
}
