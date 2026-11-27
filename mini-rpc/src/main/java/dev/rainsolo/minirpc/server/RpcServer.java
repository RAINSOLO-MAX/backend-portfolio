package dev.rainsolo.minirpc.server;

import dev.rainsolo.minirpc.common.ServiceMeta;
import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.protocol.RpcMessageDecoder;
import dev.rainsolo.minirpc.protocol.RpcMessageEncoder;
import dev.rainsolo.minirpc.registry.ServiceRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/** Netty provider. Supports ephemeral port (0) for tests and file-registry discovery for demos. */
public final class RpcServer implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RpcServer.class.getName());

    private final int requestedPort;
    private final RpcConfig config;
    private final ServiceRegistry registry;
    private final Map<String, Object> services = new ConcurrentHashMap<>();
    private final List<ServiceMeta> registered = new ArrayList<>();

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledFuture<?> heartbeatTask;
    private Channel serverChannel;
    private int boundPort;

    public RpcServer(int port, RpcConfig config, ServiceRegistry registry) {
        this.requestedPort = port;
        this.config = config;
        this.registry = registry;
    }

    public <T> void registerService(Class<T> serviceInterface, T implementation) {
        services.put(serviceInterface.getName(), implementation);
        if (serverChannel != null) {
            registerInRegistry(serviceInterface.getName());
        }
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Math.max(2, Runtime.getRuntime().availableProcessors()));
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "mini-rpc-heartbeat");
            thread.setDaemon(true);
            return thread;
        });

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                                .addLast(new IdleStateHandler(config.serverReaderIdleSeconds(), 0, 0, TimeUnit.SECONDS))
                                .addLast(new RpcMessageDecoder())
                                .addLast(new RpcMessageEncoder())
                                .addLast(new RpcServerHandler(services));
                    }
                });

        serverChannel = bootstrap.bind(requestedPort).sync().channel();
        boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        services.keySet().forEach(this::registerInRegistry);

        heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(() -> {
            for (ServiceMeta meta : List.copyOf(registered)) {
                try {
                    registry.heartbeat(meta);
                } catch (RuntimeException e) {
                    LOG.warning("Heartbeat failed: " + e.getMessage());
                }
            }
        }, 5, 5, TimeUnit.SECONDS);

        LOG.info("mini-rpc provider started on port " + boundPort + " with " + services.size() + " service(s)");
    }

    private synchronized void registerInRegistry(String serviceName) {
        ServiceMeta meta = new ServiceMeta(serviceName, "127.0.0.1", boundPort, 1, System.currentTimeMillis());
        registered.remove(meta);
        registered.add(meta);
        registry.register(meta);
    }

    public int port() {
        return boundPort;
    }

    @Override
    public synchronized void close() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        for (ServiceMeta meta : registered) {
            try {
                registry.deregister(meta);
            } catch (RuntimeException ignored) {
                // Best effort cleanup.
            }
        }
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly(2, TimeUnit.SECONDS);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        LOG.info("mini-rpc provider stopped");
    }
}
