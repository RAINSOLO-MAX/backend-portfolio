package dev.rainsolo.minirpc.client;

import dev.rainsolo.minirpc.common.RpcRequest;
import dev.rainsolo.minirpc.common.TraceContext;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** JDK dynamic proxy turning a local interface call into a remote RpcRequest. */
public final class RpcProxy implements InvocationHandler {

    private final Class<?> serviceInterface;
    private final RpcClient client;

    private RpcProxy(Class<?> serviceInterface, RpcClient client) {
        this.serviceInterface = serviceInterface;
        this.client = client;
    }

    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> serviceInterface, RpcClient client) {
        return (T) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                new RpcProxy(serviceInterface, client));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "toString" -> "RpcProxy(" + serviceInterface.getName() + ")";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> null;
            };
        }
        RpcRequest request = new RpcRequest(
                serviceInterface.getName(), method.getName(), method.getParameterTypes(), args);
        String requestKey = TraceContext.currentTraceId() == null
                ? method.getName() : TraceContext.currentTraceId();
        return client.invoke(request, requestKey);
    }
}
