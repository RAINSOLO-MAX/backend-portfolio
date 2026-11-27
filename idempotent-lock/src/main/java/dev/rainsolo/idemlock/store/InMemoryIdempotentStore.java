package dev.rainsolo.idemlock.store;

import dev.rainsolo.idemlock.core.Clock;
import dev.rainsolo.idemlock.core.IdempotencyRecord;
import dev.rainsolo.idemlock.core.SystemClock;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe in-process store. Expiry is lazy and decided with nanoTime, so a wall-clock
 * adjustment can neither resurrect nor prematurely drop a record.
 */
public final class InMemoryIdempotentStore implements IdempotentStore {

    private final ConcurrentHashMap<String, IdempotencyRecord<?>> records = new ConcurrentHashMap<>();
    private final AtomicLong expired = new AtomicLong();
    private final Clock clock;

    /** No-arg constructor required by the JDK ServiceLoader SPI. */
    public InMemoryIdempotentStore() {
        this(new SystemClock());
    }

    public InMemoryIdempotentStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public <T> IdempotencyRecord<T> get(String key) {
        IdempotencyRecord<?> found = records.get(key);
        if (found == null) {
            return null;
        }
        if (isExpired(found)) {
            if (records.remove(key, found)) {
                expired.incrementAndGet();
            }
            return null;
        }
        return cast(found);
    }

    @Override
    public <T> boolean putIfAbsent(String key, IdempotencyRecord<T> record) {
        AtomicBoolean installed = new AtomicBoolean(false);
        records.compute(key, (ignored, existing) -> {
            if (existing == null) {
                installed.set(true);
                return record;
            }
            if (isExpired(existing)) {
                expired.incrementAndGet();
                installed.set(true);
                return record;
            }
            return existing;
        });
        return installed.get();
    }

    @Override
    public <T> boolean replace(String key, String expectedOwner, IdempotencyRecord<T> record) {
        AtomicBoolean swapped = new AtomicBoolean(false);
        records.compute(key, (ignored, existing) -> {
            if (existing != null && !isExpired(existing) && existing.owner().equals(expectedOwner)) {
                swapped.set(true);
                return record;
            }
            return existing;
        });
        return swapped.get();
    }

    @Override
    public void remove(String key) {
        records.remove(key);
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public long expiredCount() {
        return expired.get();
    }

    private boolean isExpired(IdempotencyRecord<?> record) {
        return clock.nanoTime() >= record.expireAtNanos();
    }

    @SuppressWarnings("unchecked")
    private static <T> IdempotencyRecord<T> cast(IdempotencyRecord<?> record) {
        return (IdempotencyRecord<T>) record;
    }
}
