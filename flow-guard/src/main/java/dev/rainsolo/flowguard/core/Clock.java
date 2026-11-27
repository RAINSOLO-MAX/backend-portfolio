package dev.rainsolo.flowguard.core;

/** Time source. nanoTime is monotonic for refill/leak math; wallTimeMillis is kept for display. */
public interface Clock {

    long nanoTime();

    long wallTimeMillis();
}
