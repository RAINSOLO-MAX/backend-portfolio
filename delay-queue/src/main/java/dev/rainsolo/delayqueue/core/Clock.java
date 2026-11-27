package dev.rainsolo.delayqueue.core;

/** Time source. Scheduling uses nanoTime only, so wall-clock adjustments cannot corrupt deadlines. */
public interface Clock {

    long nanoTime();

    long wallTimeMillis();
}
