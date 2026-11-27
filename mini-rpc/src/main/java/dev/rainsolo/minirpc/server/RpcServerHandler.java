package dev.rainsolo.minirpc.server;

import dev.rainsolo.minirpc.common.RpcResponse;
import dev.rainsolo.minirpc.common.RpcStatus;
import dev.rainsolo.minirpc.common.TraceContext;
import dev.rainsolo.minirpc.protocol.RpcMessage;
import dev.rainsolo.minirpc.serialize.Serializer;
import dev.rainsolo.minirpc.serialize.Serializers;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class RpcServerHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger LOG = Logger.getLogger(RpcServerHandler.class.getName());

    private final Map<String, Object> services;
    private final Serializer serializer = Serializers.defaultSerializer();

    public RpcServerHandler(Map<String, Object> services) {
        this.services = services == null ? new ConcurrentHashMap<>() : services;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage message) {
        switch (message.getType()) {
            case PING -> ctx.writeAndFlush(RpcMessage.pong());
            case REQUEST -> handleRequest(ctx, message);
            default -> {
                // Responses and PONG frames are meaningless on the server side.
            }
        }
    }

    private void handleRequest(ChannelHandlerContext ctx, RpcMessage message) {
        TraceContext.restore(
                message.getHeaders().get(TraceContext.HEADER_TRACE_ID),
                message.getHeaders().get(TraceContext.HEADER_SPAN_ID));
        try {
            RpcResponse response = ServiceInvoker.invoke(services, message.getBody(), serializer);
            RpcMessage frame = new RpcMessage();
            frame.setType(dev.rainsolo.minirpc.common.MessageType.RESPONSE);
            frame.setRequestId(message.getRequestId());
            frame.setStatus(RpcStatus.fromCode(response.getStatus()));
            if (TraceContext.currentTraceId() != null) {
                frame.getHeaders().put(TraceContext.HEADER_TRACE_ID, TraceContext.currentTraceId());
            }
            try {
                frame.setBody(serializer.serialize(response));
            } catch (Exception e) {
                frame.setStatus(RpcStatus.SERVER_ERROR);
                frame.setBody(new byte[0]);
            }
            ctx.writeAndFlush(frame);
        } finally {
            TraceContext.clear();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            ctx.close();
        } else {
            super.userEventTriggered(ctx, event);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.log(Level.WARNING, "Server channel error, closing connection", cause);
        ctx.close();
    }
}
