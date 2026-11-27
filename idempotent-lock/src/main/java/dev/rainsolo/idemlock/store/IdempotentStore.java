package dev.rainsolo.idemlock.store;

import dev.rainsolo.idemlock.core.IdempotencyRecord;

/** Storage for idempotency records. Implementations must make claiming a key atomic. */
public interface IdempotentStore {

    /**
     * Reads a live record, lazily discarding an expired one.
     *
     * @return the record, or {@code null} when the key is free
     */
    <T> IdempotencyRecord<T> get(String key);

    /**
     * Atomically claims a free key. An expired record counts as free.
     *
     * @return {@code true} only for the caller that installed the record
     */
    <T> boolean putIfAbsent(String key, IdempotencyRecord<T> record);

    /**
     * Atomically swaps a record only while the given owner still holds the key.
     * This prevents a slow executor from overwriting a key reclaimed after expiry.
     *
     * @return {@code true} when the swap happened
     */
    <T> boolean replace(String key, String expectedOwner, IdempotencyRecord<T> record);

    void remove(String key);

    int size();

    /** Cumulative number of lazily discarded expired records. */
    default long expiredCount() {
        return 0;
    }
}
