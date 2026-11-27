package dev.rainsolo.idemlock.executor;

import dev.rainsolo.idemlock.core.Clock;
import dev.rainsolo.idemlock.core.ExecutionResult;
import dev.rainsolo.idemlock.core.IdempotencyRecord;
import dev.rainsolo.idemlock.core.IdempotencyState;
import dev.rainsolo.idemlock.core.IdempotentConfig;
import dev.rainsolo.idemlock.core.Outcome;
import dev.rainsolo.idemlock.core.SystemClock;
import dev.rainsolo.idemlock.store.IdempotentStore;
import dev.rainsolo.idemlock.store.InMemoryIdempotentStore;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Executes an action at most once per key. The first caller atomically claims a PROCESSING
 * record; concurrent callers either wait on its completion future and reuse the value, or
 * return RUNNING immediately. A failed record can be reclaimed for a bounded retry, and a
 * cached result can be recomputed once its TTL has elapsed.
 */
public final class IdempotentExecutor {

    private static final AtomicLong OWNER_SEQUENCE = new AtomicLong();

    private final IdempotentStore store;
    private final Clock clock;
    private final IdempotentConfig config;
    private final IdempotentMetrics metrics = new IdempotentMetrics();

    public IdempotentExecutor(IdempotentStore store, Clock clock, IdempotentConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
    }

    public IdempotentExecutor(Clock clock, IdempotentConfig config) {
        this(new InMemoryIdempotentStore(clock), clock, config);
    }

    public IdempotentExecutor(IdempotentConfig config) {
        this(new SystemClock(), config);
    }

    /**
     * Runs {@code action} under {@code key}, guaranteeing that concurrent callers of the
     * same key trigger the action only once.
     */
    public <T> ExecutionResult<T> execute(String key, Supplier<T> action) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(action, "action");
        final long startWall = clock.wallTimeMillis();
        final long expiredBefore = store.expiredCount();
        final String owner = "owner-" + OWNER_SEQUENCE.incrementAndGet()
                + "-t" + Thread.currentThread().getId();
        try {
            while (true) {
                IdempotencyRecord<T> current = store.get(key);
                if (current == null) {
                    CompletableFuture<ExecutionResult<T>> completion = new CompletableFuture<>();
                    if (!store.putIfAbsent(key, processing(owner, 1, completion))) {
                        continue;
                    }
                    return run(key, owner, 1, completion, action, startWall);
                }
                if (current.state() == IdempotencyState.SUCCEEDED) {
                    metrics.incReused();
                    return ExecutionResult.reused(current.value(), elapsed(startWall));
                }
                if (current.state() == IdempotencyState.PROCESSING) {
                    return await(current, startWall);
                }
                int attempt = current.attemptCount() + 1;
                if (attempt > config.maxAttempts()) {
                    metrics.incFailed();
                    return ExecutionResult.failed(
                            "max attempts exceeded after " + current.attemptCount() + " attempts",
                            elapsed(startWall));
                }
                CompletableFuture<ExecutionResult<T>> completion = new CompletableFuture<>();
                if (!store.replace(key, current.owner(), processing(owner, attempt, completion))) {
                    continue;
                }
                return run(key, owner, attempt, completion, action, startWall);
            }
        } finally {
            metrics.addExpired(store.expiredCount() - expiredBefore);
        }
    }

    private <T> ExecutionResult<T> run(String key, String owner, int attempt,
                                       CompletableFuture<ExecutionResult<T>> completion,
                                       Supplier<T> action, long startWall) {
        ExecutionResult<T> result;
        try {
            T value = action.get();
            result = ExecutionResult.executed(value, elapsed(startWall));
            metrics.incExecuted();
        } catch (RuntimeException e) {
            result = ExecutionResult.failed(describe(e), elapsed(startWall));
            metrics.incFailed();
        }
        IdempotencyState terminalState = result.outcome() == Outcome.EXECUTED
                ? IdempotencyState.SUCCEEDED
                : IdempotencyState.FAILED;
        long deadline = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.ttlMillis());
        IdempotencyRecord<T> terminal = new IdempotencyRecord<>(
                terminalState, result.value(), deadline, owner, attempt, completion);
        store.replace(key, owner, terminal);
        completion.complete(result);
        return result;
    }

    private <T> ExecutionResult<T> await(IdempotencyRecord<T> current, long startWall) {
        if (!config.waitForResult()) {
            metrics.incRunning();
            return ExecutionResult.running(elapsed(startWall));
        }
        try {
            ExecutionResult<T> result = current.completion()
                    .get(config.waitTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (result.outcome() == Outcome.EXECUTED) {
                metrics.incReused();
                return ExecutionResult.reused(result.value(), elapsed(startWall));
            }
            metrics.incFailed();
            return ExecutionResult.failed(result.failureMessage(), elapsed(startWall));
        } catch (TimeoutException e) {
            metrics.incRunning();
            return ExecutionResult.running(elapsed(startWall));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.incFailed();
            return ExecutionResult.failed("interrupted while waiting", elapsed(startWall));
        } catch (ExecutionException e) {
            metrics.incFailed();
            return ExecutionResult.failed(describe(e.getCause()), elapsed(startWall));
        }
    }

    /**
     * A processing placeholder outlives one waiting cycle, so a slow action cannot be
     * executed a second time simply because the cache TTL is short.
     */
    private <T> IdempotencyRecord<T> processing(String owner, int attempt,
                                                CompletableFuture<ExecutionResult<T>> completion) {
        long leaseMillis = config.ttlMillis() + config.waitTimeoutMillis();
        long deadline = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMillis);
        return new IdempotencyRecord<>(
                IdempotencyState.PROCESSING, null, deadline, owner, attempt, completion);
    }

    private long elapsed(long startWall) {
        return Math.max(0L, clock.wallTimeMillis() - startWall);
    }

    private static String describe(Throwable error) {
        String message = error.getMessage();
        return message == null ? error.getClass().getSimpleName() : message;
    }

    public IdempotentMetrics metrics() {
        return metrics;
    }

    public IdempotentStore store() {
        return store;
    }

    public IdempotentConfig config() {
        return config;
    }
}
