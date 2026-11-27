package dev.rainsolo.minirpc.common;

/** Frame type. Heartbeat frames carry an empty body and no round-trip id correlation. */
public enum MessageType {

    REQUEST(0),
    RESPONSE(1),
    PING(2),
    PONG(3);

    private final int code;

    MessageType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static MessageType fromCode(int code) {
        for (MessageType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown message type code: " + code);
    }
}
