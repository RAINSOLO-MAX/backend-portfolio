package dev.rainsolo.minirpc.common;

import java.io.Serializable;
import java.util.Arrays;

/** Request payload transported inside an RPC frame. */
public final class RpcRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private String serviceName;
    private String methodName;
    private Class<?>[] parameterTypes;
    private Object[] parameters;

    public RpcRequest() {
    }

    public RpcRequest(String serviceName, String methodName, Class<?>[] parameterTypes, Object[] parameters) {
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
        this.parameters = parameters == null ? new Object[0] : parameters;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public Object[] getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return "RpcRequest{" + serviceName + "#" + methodName + "(" + Arrays.toString(parameterTypes) + ")}";
    }
}
