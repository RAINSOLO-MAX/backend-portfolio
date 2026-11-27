package dev.rainsolo.idemlock.lock;

import dev.rainsolo.idemlock.core.Clock;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Reentrant lease lock. One instance is bound to the thread that created it; that thread may
 * acquire it repeatedly, and only the last unlock releases the lease. A daemon watchdog renews
 * the lease every {@code lease / 3} while the lock is held, and stops on the final unlock.
 */
public final class ReentrantDistributedLock implements DistributedLock {

    private static final AtomicLong OWNER_SEQUENCE = new AtomicLong();

    private final String name;
    private final LockStore store;
    private final Clock clock;
    private final LockMetrics metrics;
    private final boolean watchdogEnabled;
    private final String ownerId;
    private final Thread ownerThread;

    private int holdCount;
    private long token = -1L;
    private long leaseNanos;
    private long expireAtNanos = Long.MIN_VALUE;
    private long lastSeenExpired;
    private volatile boolean watchdogActive;
    private Thread watchdogThread;

    public ReentrantDistributedLock(String name, LockStore store, Clock clock,
                                    LockMetrics metrics, boolean watchdogEnabled) {
        this.name = Objects.requireNonNull(name, "name");
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.watchdogEnabled = watchdogEnabled;
        this.ownerThread = Thread.currentThread();
        this.ownerId = name + "-" + OWNER_SEQUENCE.incrementAndGet()
                + "-t" + ownerThread.getId();
        this.lastSeenExpired = store.expiredCount();
    }

    @Override
    public boolean tryLock(long leaseMillis) {
        validateLease(leaseMillis);
        requireOwnerThread();
        syncExpired();
        if (holdCount > 0 && stillHeld()) {
            holdCount++;
            return true;
        }
        long candidateLease = TimeUnit.MILLISECONDS.toNanos(leaseMillis);
        long granted = store.acquire(name, ownerId, candidateLease);
        if (granted < 0) {
            metrics.incRejected();
            return false;
        }
        token = granted;
        leaseNanos = candidateLease;
        expireAtNanos = clock.nanoTime() + candidateLease;
        holdCount = 1;
        metrics.incAcquired();
        if (watchdogEnabled) {
            startWatchdog(candidateLease);
        }
        return true;
    }

    @Override
    public boolean tryLock(long leaseMillis, long waitMillis) {
        validateLease(leaseMillis);
        if (waitMillis < 0) {
            throw new IllegalArgumentException("waitMillis must not be negative");
        }
        // Retry budget is real time: the caller asked to wait, not to advance a virtual clock.
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        boolean interrupted = false;
        try {
            while (true) {
                if (tryLock(leaseMillis)) {
                    return true;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                long sleepNanos = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(5));
                try {
                    Thread.sleep(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(sleepNanos)));
                } catch (InterruptedException e) {
                    interrupted = true;
                    return false;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void unlock() {
        requireOwnerThread();
        if (holdCount == 0) {
            throw new IllegalMonitorStateException("lock not held: " + name);
        }
        holdCount--;
        if (holdCount > 0) {
            return;
        }
        stopWatchdog();
        if (!store.release(name, ownerId)) {
            metrics.incExpired();
        }
        metrics.incReleased();
        token = -1L;
        expireAtNanos = Long.MIN_VALUE;
        syncExpired();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return Thread.currentThread() == ownerThread && stillHeld();
    }

    @Override
    public long fencingToken() {
        return isHeldByCurrentThread() ? token : -1L;
    }

    @Override
    public int holdCount() {
        if (Thread.currentThread() != ownerThread || !stillHeld()) {
            return 0;
        }
        return holdCount;
    }

    @Override
    public boolean renew() {
        requireOwnerThread();
        if (holdCount == 0) {
            return false;
        }
        return renewLease(leaseNanos);
    }

    private boolean renewLease(long renewNanos) {
        if (!store.renew(name, ownerId, renewNanos)) {
            return false;
        }
        expireAtNanos = clock.nanoTime() + renewNanos;
        metrics.incRenewed();
        return true;
    }

    private void startWatchdog(long watchdogLeaseNanos) {
        long intervalNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(1), watchdogLeaseNanos / 3);
        watchdogActive = true;
        Thread thread = new Thread(() -> {
            while (watchdogActive) {
                LockSupport.parkNanos(intervalNanos);
                if (!watchdogActive) {
                    return;
                }
                if (!renewLease(watchdogLeaseNanos)) {
                    watchdogActive = false;
                    return;
                }
            }
        }, "idemlock-watchdog-" + name);
        thread.setDaemon(true);
        watchdogThread = thread;
        thread.start();
    }

    private void stopWatchdog() {
        watchdogActive = false;
        Thread thread = watchdogThread;
        watchdogThread = null;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private boolean stillHeld() {
        if (holdCount == 0 || clock.nanoTime() >= expireAtNanos) {
            return false;
        }
        String holder = store.holder(name);
        return holder != null && holder.equals(ownerId);
    }

    private void syncExpired() {
        long current = store.expiredCount();
        long delta = current - lastSeenExpired;
        if (delta > 0) {
            metrics.addExpired(delta);
            lastSeenExpired = current;
        }
    }

    private void requireOwnerThread() {
        if (Thread.currentThread() != ownerThread) {
            throw new IllegalMonitorStateException(
                    "lock " + name + " belongs to thread " + ownerThread.getName());
        }
    }

    private static void validateLease(long leaseMillis) {
        if (leaseMillis <= 0) {
            throw new IllegalArgumentException("leaseMillis must be positive");
        }
    }
}
