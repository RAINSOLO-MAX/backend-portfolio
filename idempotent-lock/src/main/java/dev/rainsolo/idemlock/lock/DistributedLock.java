package dev.rainsolo.idemlock.lock;

/** A named, reentrant, lease-based lock with a monotonic fencing token. */
public interface DistributedLock {

    /** Tries once to acquire the lock for the given lease, returning immediately. */
    boolean tryLock(long leaseMillis);

    /** Tries to acquire the lock, retrying until the wait budget is exhausted. */
    boolean tryLock(long leaseMillis, long waitMillis);

    /** Releases one reentrancy level; the last level releases the lease. */
    void unlock();

    boolean isHeldByCurrentThread();

    /** Returns the fencing token, or {@code -1} when the lock is not held. */
    long fencingToken();

    int holdCount();

    /** Extends the lease once; returns {@code false} when the lease was already lost. */
    boolean renew();
}
