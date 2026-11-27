package dev.rainsolo.scheduler.scheduler;

/** Clock backed by the JVM wall clock and monotonic nanosecond source. */
public final class SystemClock implements Clock {

    @Override
    public long wallTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }
}
