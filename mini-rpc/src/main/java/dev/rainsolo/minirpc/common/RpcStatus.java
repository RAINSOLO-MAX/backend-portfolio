package dev.rainsolo.minirpc.common;

/** Status code carried in the frame header, independent of the serialized body. */
public enum RpcStatus {

    OK(0),
    BAD_REQUEST(1),
    NOT_FOUND(2),
    SERVER_ERROR(3),
    TIMEOUT(4),
    CIRCUIT_OPEN(5);

    private final int code;

    RpcStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static RpcStatus fromCode(int code) {
        for (RpcStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown RPC status code: " + code);
    }
}
