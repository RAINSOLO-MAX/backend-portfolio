package dev.rainsolo.minirpc.client;

import dev.rainsolo.minirpc.common.RpcResponse;
import dev.rainsolo.minirpc.common.RpcStatus;
import dev.rainsolo.minirpc.protocol.RpcMessage;
import dev.rainsolo.minirpc.serialize.Serializer;
import dev.rainsolo.minirpc.serialize.Serializers;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Correlates responses back to the waiting caller by requestId; owns that channel's in-flight calls. */
public final class RpcClientHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger LOG = Logger.getLogger(RpcClientHandler.class.getName());

    private final ConcurrentMap<Long, CompletableFuture<RpcResponse>> pending = new ConcurrentHashMap<>();
    private final Serializer serializer = Serializers.defaultSerializer();
    private volatile long lastPongMillis = System.currentTimeMillis();

    public CompletableFuture<RpcResponse> register(long requestId) {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        pending.put(requestId, future);
        return future;
    }

    public void remove(long requestId) {
        pending.remove(requestId);
    }

    public long lastPongMillis() {
        return lastPongMillis;
    }

    public int inFlight() {
        return pending.size();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage message) {
        switch (message.getType()) {
            case RESPONSE -> {
                CompletableFuture<RpcResponse> future = pending.remove(message.getRequestId());
                if (future == null) {
                    return;
                }
                if (message.getStatus() != RpcStatus.OK && message.getBody().length == 0) {
                    future.complete(RpcResponse.error(message.getStatus(), "Transport-level failure", null));
                    return;
                }
                try {
                    future.complete(serializer.deserialize(message.getBody(), RpcResponse.class));
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }
            case PING -> ctx.writeAndFlush(RpcMessage.pong());
            case PONG -> lastPongMillis = System.currentTimeMillis();
            default -> {
                // REQUEST frames are not expected on a client channel.
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent idle && idle.state() == IdleState.WRITER_IDLE) {
            ctx.writeAndFlush(RpcMessage.ping());
        } else {
            super.userEventTriggered(ctx, event);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        IOException closed = new IOException("Connection closed before response arrived");
        pending.values().forEach(future -> future.completeExceptionally(closed));
        pending.clear();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.log(Level.WARNING, "Client channel error", cause);
        ctx.close();
    }
}
