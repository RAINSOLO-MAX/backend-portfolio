package dev.rainsolo.minirpc.client;

import dev.rainsolo.minirpc.common.ServiceMeta;
import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.protocol.RpcMessageDecoder;
import dev.rainsolo.minirpc.protocol.RpcMessageEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** One long-lived Netty channel per provider endpoint, reused across all calls. */
public final class ConnectionPool implements AutoCloseable {

    private final Bootstrap bootstrap;
    private final Map<String, Channel> channels = new ConcurrentHashMap<>();

    public ConnectionPool(NioEventLoopGroup group, RpcConfig config) {
        bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                                .addLast(new IdleStateHandler(0, config.heartbeatIntervalSeconds(), 0, TimeUnit.SECONDS))
                                .addLast(new RpcMessageDecoder())
                                .addLast(new RpcMessageEncoder())
                                .addLast(new RpcClientHandler());
                    }
                });
    }

    public Channel get(ServiceMeta provider) throws InterruptedException {
        String key = provider.endpoint();
        Channel existing = channels.get(key);
        if (existing != null && existing.isActive()) {
            return existing;
        }
        synchronized (channels) {
            existing = channels.get(key);
            if (existing != null && existing.isActive()) {
                return existing;
            }
            if (existing != null) {
                existing.close();
            }
            Channel fresh = bootstrap.connect(provider.getHost(), provider.getPort()).sync().channel();
            channels.put(key, fresh);
            return fresh;
        }
    }

    public int size() {
        return (int) channels.values().stream().filter(Channel::isActive).count();
    }

    @Override
    public void close() {
        channels.values().forEach(channel -> channel.close().awaitUninterruptibly(1, TimeUnit.SECONDS));
        channels.clear();
    }
}
