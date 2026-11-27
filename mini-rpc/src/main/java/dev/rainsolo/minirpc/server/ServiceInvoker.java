package dev.rainsolo.minirpc.server;

import dev.rainsolo.minirpc.common.RpcRequest;
import dev.rainsolo.minirpc.common.RpcResponse;
import dev.rainsolo.minirpc.common.RpcStatus;
import dev.rainsolo.minirpc.serialize.Serializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/** Deserializes the request and invokes the local bean reflectively. */
public final class ServiceInvoker {

    private ServiceInvoker() {
    }

    public static RpcResponse invoke(Map<String, Object> services, byte[] body, Serializer serializer) {
        try {
            RpcRequest request = serializer.deserialize(body, RpcRequest.class);
            Object bean = services.get(request.getServiceName());
            if (bean == null) {
                return RpcResponse.error(RpcStatus.NOT_FOUND,
                        "No provider for " + request.getServiceName(), null);
            }
            Method method = bean.getClass().getMethod(request.getMethodName(), request.getParameterTypes());
            Object result = method.invoke(bean, request.getParameters());
            return RpcResponse.ok(result);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            return RpcResponse.error(RpcStatus.SERVER_ERROR,
                    "Provider threw " + cause.getClass().getName() + ": " + cause.getMessage(), cause);
        } catch (NoSuchMethodException e) {
            return RpcResponse.error(RpcStatus.BAD_REQUEST, "Method not found: " + e.getMessage(), e);
        } catch (Exception e) {
            return RpcResponse.error(RpcStatus.SERVER_ERROR, "Invocation failed: " + e.getMessage(), e);
        }
    }
}
