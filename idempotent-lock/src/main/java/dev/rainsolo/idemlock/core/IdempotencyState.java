package dev.rainsolo.idemlock.core;

/** State of an idempotency record. */
public enum IdempotencyState {
    /** A caller owns the key and is executing the action. */
    PROCESSING,
    /** The action succeeded and its value is cached until the TTL expires. */
    SUCCEEDED,
    /** The action failed; a later caller may reclaim the key and retry. */
    FAILED
}
