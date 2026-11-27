package dev.rainsolo.scheduler.scheduler;

/** Lifecycle state of one shard execution in the idempotent ledger. */
public enum ExecutionState {

    PENDING,
    RUNNING,
    SUCCESS,
    FAILED,
    TIMEOUT
}
