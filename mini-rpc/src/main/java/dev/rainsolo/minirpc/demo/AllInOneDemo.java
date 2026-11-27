package dev.rainsolo.minirpc.demo;

import dev.rainsolo.minirpc.client.RpcClient;
import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.registry.InMemoryRegistry;
import dev.rainsolo.minirpc.server.RpcServer;

/** Single-JVM smoke demo: start provider + consumer, issue a few real network calls. */
public final class AllInOneDemo {

    public static void main(String[] args) throws Exception {
        InMemoryRegistry registry = new InMemoryRegistry();
        RpcServer server = new RpcServer(0, RpcConfig.defaults(), registry);
        server.registerService(DemoService.class, new DemoServiceImpl());
        server.start();

        try (RpcClient client = new RpcClient(RpcConfig.defaults(), registry)) {
            DemoService service = client.createProxy(DemoService.class);
            System.out.println("echo       -> " + service.echo("hello"));
            System.out.println("add        -> " + service.add(17, 25));
            System.out.println("traceId    -> " + service.traceId());
            System.out.println("connections-> " + client.connectionPool().size() + " (one reused channel)");
        } finally {
            server.close();
            registry.close();
        }
    }
}
