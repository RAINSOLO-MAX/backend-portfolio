package dev.rainsolo.scheduler.scheduler;

/** Time source. Tests and demos inject a mutable clock; production uses {@link SystemClock}. */
public interface Clock {

    long wallTimeMillis();

    long nanoTime();
}
