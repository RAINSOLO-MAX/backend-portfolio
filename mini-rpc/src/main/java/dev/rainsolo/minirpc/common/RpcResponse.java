package dev.rainsolo.minirpc.common;

import java.io.Serializable;

/** Response payload transported inside an RPC frame. */
public final class RpcResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private int status;
    private String message;
    private Object result;
    private String errorClass;

    public RpcResponse() {
    }

    public static RpcResponse ok(Object result) {
        RpcResponse response = new RpcResponse();
        response.status = RpcStatus.OK.getCode();
        response.result = result;
        response.message = "OK";
        return response;
    }

    public static RpcResponse error(RpcStatus status, String message, Throwable cause) {
        RpcResponse response = new RpcResponse();
        response.status = status.getCode();
        response.message = message;
        response.errorClass = cause == null ? null : cause.getClass().getName();
        return response;
    }

    public boolean isSuccess() {
        return status == RpcStatus.OK.getCode();
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public Object getResult() {
        return result;
    }

    public String getErrorClass() {
        return errorClass;
    }
}
