package dev.rainsolo.idemlock.core;

/**
 * Result of an idempotent execution.
 *
 * @param outcome        how the result was obtained
 * @param value          action result, or {@code null} when none is available
 * @param failureMessage failure detail, or {@code null} on success
 * @param elapsedMillis  wall-clock duration of this call
 * @param <T>            result type
 */
public record ExecutionResult<T>(Outcome outcome, T value, String failureMessage, long elapsedMillis) {

    public static <T> ExecutionResult<T> executed(T value, long elapsedMillis) {
        return new ExecutionResult<>(Outcome.EXECUTED, value, null, elapsedMillis);
    }

    public static <T> ExecutionResult<T> reused(T value, long elapsedMillis) {
        return new ExecutionResult<>(Outcome.REUSED, value, null, elapsedMillis);
    }

    public static <T> ExecutionResult<T> running(long elapsedMillis) {
        return new ExecutionResult<>(Outcome.RUNNING, null, null, elapsedMillis);
    }

    public static <T> ExecutionResult<T> failed(String message, long elapsedMillis) {
        return new ExecutionResult<>(Outcome.FAILED, null, message, elapsedMillis);
    }
}
