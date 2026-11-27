package dev.rainsolo.scheduler.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Idempotent execution ledger keyed by {@code jobName#fireTimeMillis#shardItem}.
 * A SUCCESS, RUNNING or TIMEOUT-that-has-exhausted-attempts record is suppressed;
 * FAILED/TIMEOUT records may be re-claimed until {@code maxAttempts} is reached.
 */
public final class ExecutionLedger {

    private final ConcurrentHashMap<String, ExecutionRecord> records = new ConcurrentHashMap<>();
    private final AtomicLong duplicatesSuppressed = new AtomicLong();
    private final int maxAttempts;

    public ExecutionLedger(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        this.maxAttempts = maxAttempts;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Claims the execution slot for a candidate record.
     *
     * @return the record that should actually be executed (the stored one when retrying),
     *         or {@code null} when the key was already handled and is suppressed
     */
    public ExecutionRecord tryClaim(ExecutionRecord candidate) {
        ExecutionRecord stored = records.putIfAbsent(candidate.key(), candidate);
        if (stored == null) {
            candidate.setState(ExecutionState.RUNNING);
            candidate.incrementAttempts();
            return candidate;
        }
        synchronized (stored) {
            ExecutionState state = stored.state();
            boolean retryable = (state == ExecutionState.FAILED || state == ExecutionState.TIMEOUT)
                    && stored.attempts() < maxAttempts;
            if (retryable) {
                stored.setState(ExecutionState.RUNNING);
                stored.incrementAttempts();
                return stored;
            }
        }
        duplicatesSuppressed.incrementAndGet();
        return null;
    }

    public void markSuccess(ExecutionRecord record) {
        record.setState(ExecutionState.SUCCESS);
    }

    public void markFailed(ExecutionRecord record) {
        record.setState(ExecutionState.FAILED);
    }

    public void markTimeout(ExecutionRecord record) {
        record.setState(ExecutionState.TIMEOUT);
    }

    /** Number of duplicate claims suppressed by this ledger. */
    public long duplicatesSuppressed() {
        return duplicatesSuppressed.get();
    }

    public ExecutionRecord get(String key) {
        return records.get(key);
    }

    public int size() {
        return records.size();
    }
}
