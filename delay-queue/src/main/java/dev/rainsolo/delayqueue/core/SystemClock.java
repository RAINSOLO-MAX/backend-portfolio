package dev.rainsolo.delayqueue.core;

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
