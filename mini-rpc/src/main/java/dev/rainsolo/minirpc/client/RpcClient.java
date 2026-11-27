package dev.rainsolo.minirpc.client;

import dev.rainsolo.minirpc.common.MessageType;
import dev.rainsolo.minirpc.common.RpcException;
import dev.rainsolo.minirpc.common.RpcRequest;
import dev.rainsolo.minirpc.common.RpcResponse;
import dev.rainsolo.minirpc.common.RpcStatus;
import dev.rainsolo.minirpc.common.ServiceMeta;
import dev.rainsolo.minirpc.common.TraceContext;
import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.fault.CircuitBreaker;
import dev.rainsolo.minirpc.fault.CircuitBreakers;
import dev.rainsolo.minirpc.fault.RetryPolicy;
import dev.rainsolo.minirpc.loadbalance.LoadBalance;
import dev.rainsolo.minirpc.loadbalance.LoadBalances;
import dev.rainsolo.minirpc.protocol.RpcMessage;
import dev.rainsolo.minirpc.registry.ServiceRegistry;
import dev.rainsolo.minirpc.serialize.Serializer;
import dev.rainsolo.minirpc.serialize.Serializers;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/** Consumer entry point: discovery -> load balancing -> circuit breaker -> retry -> send. */
public final class RpcClient implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RpcClient.class.getName());

    private final RpcConfig config;
    private final ServiceRegistry registry;
    private final Serializer serializer = Serializers.defaultSerializer();
    private final LoadBalance loadBalance;
    private final RetryPolicy retryPolicy;
    private final CircuitBreakers breakers;
    private final NioEventLoopGroup group;
    private final ConnectionPool pool;
    private final AtomicLong requestIds = new AtomicLong();

    public RpcClient(RpcConfig config, ServiceRegistry registry) {
        this.config = config;
        this.registry = registry;
        this.loadBalance = LoadBalances.of(config.loadBalance());
        this.retryPolicy = new RetryPolicy(config.maxAttempts(), 20, 2.0, 500);
        this.breakers = new CircuitBreakers(config.failureThreshold(), config.windowSize(), config.circuitOpenMillis());
        this.group = new NioEventLoopGroup(Math.max(2, Runtime.getRuntime().availableProcessors()));
        this.pool = new ConnectionPool(group, config);
    }

    public <T> T createProxy(Class<T> serviceInterface) {
        return RpcProxy.create(serviceInterface, this);
    }

    /** Full invocation pipeline. Called by the dynamic proxy. */
    public Object invoke(RpcRequest request, String requestKey) {
        String serviceName = request.getServiceName();
        String traceId = TraceContext.traceIdOrNew();
        String spanId = TraceContext.nextSpanId();
        RpcException lastFailure = null;

        for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
            List<ServiceMeta> providers = registry.lookup(serviceName);
            if (providers.isEmpty()) {
                throw RpcException.transport(RpcStatus.NOT_FOUND,
                        "No live provider for " + serviceName, null);
            }
            ServiceMeta target = loadBalance.select(serviceName, providers, requestKey);
            CircuitBreaker breaker = breakers.get(serviceName + "@" + target.endpoint());

            if (!breaker.allowRequest()) {
                lastFailure = RpcException.transport(RpcStatus.CIRCUIT_OPEN,
                        "Circuit open for " + target.endpoint(), null);
                if (!retryPolicy.shouldRetry(lastFailure, attempt)) {
                    throw lastFailure;
                }
                sleepBackoff(attempt);
                continue;
            }

            try {
                RpcResponse response = sendOnce(request, target, traceId, spanId);
                breaker.recordSuccess();
                if (response.isSuccess()) {
                    return response.getResult();
                }
                throw RpcException.business(RpcStatus.fromCode(response.getStatus()),
                        response.getMessage() == null ? "Provider error" : response.getMessage());
            } catch (RpcException e) {
                if (!e.isRetryable()) {
                    throw e;
                }
                breaker.recordFailure();
                lastFailure = e;
            }

            if (!retryPolicy.shouldRetry(lastFailure, attempt)) {
                throw lastFailure;
            }
            sleepBackoff(attempt);
        }
        throw lastFailure == null
                ? RpcException.transport(RpcStatus.TIMEOUT, "Invocation failed", null)
                : lastFailure;
    }

    private RpcResponse sendOnce(RpcRequest request, ServiceMeta target, String traceId, String spanId) {
        long requestId = requestIds.incrementAndGet();
        Channel channel;
        RpcClientHandler handler;
        CompletableFuture<RpcResponse> future;
        try {
            channel = pool.get(target);
            handler = channel.pipeline().get(RpcClientHandler.class);
            future = handler.register(requestId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RpcException.transport(RpcStatus.SERVER_ERROR, "Interrupted while connecting", e);
        } catch (Exception e) {
            throw RpcException.transport(RpcStatus.SERVER_ERROR,
                    "Connect to " + target.endpoint() + " failed", e);
        }

        try {
            RpcMessage frame = new RpcMessage();
            frame.setType(MessageType.REQUEST);
            frame.setRequestId(requestId);
            frame.getHeaders().put(TraceContext.HEADER_TRACE_ID, traceId);
            frame.getHeaders().put(TraceContext.HEADER_SPAN_ID, spanId);
            frame.setBody(serializer.serialize(request));

            channel.writeAndFlush(frame).addListener(write -> {
                if (!write.isSuccess()) {
                    future.completeExceptionally(write.cause());
                }
            });
            return future.get(config.requestTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw RpcException.transport(RpcStatus.TIMEOUT,
                    "Call timed out after " + config.requestTimeoutMillis() + "ms", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RpcException.transport(RpcStatus.SERVER_ERROR, "Interrupted while waiting", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw RpcException.transport(RpcStatus.SERVER_ERROR,
                    "Transport failure: " + cause.getMessage(), cause);
        } catch (IOException e) {
            throw RpcException.transport(RpcStatus.SERVER_ERROR, "Serialization failed", e);
        } finally {
            handler.remove(requestId);
        }
    }

    private void sleepBackoff(int attempt) {
        long backoff = retryPolicy.backoffMillis(attempt);
        if (backoff <= 0) {
            return;
        }
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public ConnectionPool connectionPool() {
        return pool;
    }

    public CircuitBreakers circuitBreakers() {
        return breakers;
    }

    @Override
    public void close() {
        pool.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        LOG.info("mini-rpc consumer stopped");
    }
}
