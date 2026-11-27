package dev.rainsolo.idemlock.core;

import java.util.concurrent.CompletableFuture;

/**
 * Immutable snapshot stored under an idempotency key. State transitions publish a new record
 * that keeps the same completion future, so waiters are notified exactly once.
 *
 * @param state          current state
 * @param value          cached result value, or {@code null} while processing or after a failure
 * @param expireAtNanos  monotonic deadline after which the record is treated as absent
 * @param owner          identity of the caller that claimed the key
 * @param attemptCount   number of execution attempts already made for this key
 * @param completion     future completed when the owning caller finishes the action
 * @param <T>            result type
 */
public record IdempotencyRecord<T>(
        IdempotencyState state,
        T value,
        long expireAtNanos,
        String owner,
        int attemptCount,
        CompletableFuture<ExecutionResult<T>> completion) {
}
