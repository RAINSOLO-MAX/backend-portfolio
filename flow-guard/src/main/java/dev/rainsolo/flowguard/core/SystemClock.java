package dev.rainsolo.flowguard.core;

/** Default clock backed by the JDK monotonic and wall clocks. */
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
