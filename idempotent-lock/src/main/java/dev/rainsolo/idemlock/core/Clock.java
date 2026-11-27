package dev.rainsolo.idemlock.core;

/** Time source. Lease and TTL decisions use nanoTime only, so wall-clock adjustments cannot corrupt deadlines. */
public interface Clock {

    long nanoTime();

    long wallTimeMillis();
}
