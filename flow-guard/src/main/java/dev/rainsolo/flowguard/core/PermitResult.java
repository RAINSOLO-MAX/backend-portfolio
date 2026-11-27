package dev.rainsolo.flowguard.core;

/** Outcome of a permit request, including how long a rejected caller should wait. */
public record PermitResult(boolean allowed, long waitMillis, long remainingPermits) {

    public static PermitResult permit(long remainingPermits) {
        return new PermitResult(true, 0L, Math.max(0L, remainingPermits));
    }

    public static PermitResult reject(long waitMillis) {
        return new PermitResult(false, Math.max(1L, waitMillis), 0L);
    }

    public long retryAfterMillis() {
        return waitMillis;
    }
}
