package dev.rainsolo.minirpc.common;

/** Client-side exception; {@code retryable} drives the retry policy. */
public class RpcException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final RpcStatus status;
    private final boolean retryable;

    public RpcException(RpcStatus status, String message) {
        this(status, message, null, false);
    }

    public RpcException(RpcStatus status, String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.status = status;
        this.retryable = retryable;
    }

    public RpcStatus getStatus() {
        return status;
    }

    /** Only transport-level failures are retried; business errors are surfaced immediately. */
    public boolean isRetryable() {
        return retryable;
    }

    public static RpcException transport(RpcStatus status, String message, Throwable cause) {
        return new RpcException(status, message, cause, true);
    }

    public static RpcException business(RpcStatus status, String message) {
        return new RpcException(status, message, null, false);
    }
}
