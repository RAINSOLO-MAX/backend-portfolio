package dev.rainsolo.idemlock.lock;

import dev.rainsolo.idemlock.core.Clock;
import dev.rainsolo.idemlock.core.SystemClock;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process lock store. Each name owns an independent AtomicLong, so fencing tokens are
 * strictly increasing per lock name. Lease expiry is decided only with nanoTime.
 */
public final class InMemoryLockStore implements LockStore {

    private final ConcurrentHashMap<String, Lease> leases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> tokens = new ConcurrentHashMap<>();
    private final AtomicLong expired = new AtomicLong();
    private final Clock clock;

    public InMemoryLockStore() {
        this(new SystemClock());
    }

    public InMemoryLockStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public long acquire(String name, String owner, long leaseNanos) {
        long now = clock.nanoTime();
        AtomicLong granted = new AtomicLong(-1L);
        leases.compute(name, (ignored, existing) -> {
            if (existing != null && existing.expireAtNanos() <= now) {
                expired.incrementAndGet();
                existing = null;
            }
            if (existing == null) {
                long token = nextToken(name);
                granted.set(token);
                return new Lease(owner, now + leaseNanos, token);
            }
            return existing;
        });
        return granted.get();
    }

    @Override
    public boolean renew(String name, String owner, long leaseNanos) {
        long now = clock.nanoTime();
        AtomicBoolean renewed = new AtomicBoolean(false);
        leases.compute(name, (ignored, existing) -> {
            if (existing != null && existing.expireAtNanos() > now && existing.owner().equals(owner)) {
                renewed.set(true);
                return new Lease(owner, now + leaseNanos, existing.token());
            }
            return existing;
        });
        return renewed.get();
    }

    @Override
    public boolean release(String name, String owner) {
        long now = clock.nanoTime();
        AtomicBoolean released = new AtomicBoolean(false);
        leases.compute(name, (ignored, existing) -> {
            if (existing != null && existing.expireAtNanos() > now && existing.owner().equals(owner)) {
                released.set(true);
                return null;
            }
            return existing;
        });
        return released.get();
    }

    @Override
    public String holder(String name) {
        Lease lease = leases.get(name);
        if (lease == null) {
            return null;
        }
        if (lease.expireAtNanos() <= clock.nanoTime()) {
            if (leases.remove(name, lease)) {
                expired.incrementAndGet();
            }
            return null;
        }
        return lease.owner();
    }

    @Override
    public long nextToken(String name) {
        return tokens.computeIfAbsent(name, ignored -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public long expiredCount() {
        return expired.get();
    }

    private record Lease(String owner, long expireAtNanos, long token) {
    }
}
