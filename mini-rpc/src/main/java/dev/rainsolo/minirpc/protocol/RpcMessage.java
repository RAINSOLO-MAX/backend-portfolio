package dev.rainsolo.minirpc.protocol;

import dev.rainsolo.minirpc.common.MessageType;
import dev.rainsolo.minirpc.common.RpcStatus;

import java.util.HashMap;
import java.util.Map;

/** In-memory representation of one wire frame. */
public final class RpcMessage {

    private byte version = 1;
    private MessageType type = MessageType.REQUEST;
    private RpcStatus status = RpcStatus.OK;
    private long requestId;
    private Map<String, String> headers = new HashMap<>();
    private byte[] body = new byte[0];

    public static RpcMessage ping() {
        RpcMessage message = new RpcMessage();
        message.setType(MessageType.PING);
        return message;
    }

    public static RpcMessage pong() {
        RpcMessage message = new RpcMessage();
        message.setType(MessageType.PONG);
        return message;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public RpcStatus getStatus() {
        return status;
    }

    public void setStatus(RpcStatus status) {
        this.status = status;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers == null ? new HashMap<>() : headers;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body == null ? new byte[0] : body;
    }
}
