package dev.rainsolo.idemlock.core;

/** The outcome of one call to the idempotent executor. */
public enum Outcome {
    /** This caller claimed the key and executed the action. */
    EXECUTED,
    /** This caller reused a result produced by another caller. */
    REUSED,
    /** Another caller is still processing the key and no result was available yet. */
    RUNNING,
    /** The action failed, or the caller gave up waiting. */
    FAILED
}
