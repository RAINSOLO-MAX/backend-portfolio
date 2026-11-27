package dev.rainsolo.idemlock.lock;

/** Storage primitive behind a distributed lock. All operations must be atomic per name. */
public interface LockStore {

    /**
     * Acquires a free (or expired) name for the given owner for a monotonic lease duration.
     *
     * @return the newly issued fencing token, or {@code -1} when the name is held
     */
    long acquire(String name, String owner, long leaseNanos);

    /** Extends the lease only while the owner still holds it. */
    boolean renew(String name, String owner, long leaseNanos);

    /** Releases the lease only while the owner still holds it. */
    boolean release(String name, String owner);

    /** Returns the current owner, or {@code null} when the lease is free or expired. */
    String holder(String name);

    /** Issues the next strictly increasing fencing token for a name. */
    long nextToken(String name);

    /** Cumulative number of leases observed as expired. */
    default long expiredCount() {
        return 0;
    }
}
