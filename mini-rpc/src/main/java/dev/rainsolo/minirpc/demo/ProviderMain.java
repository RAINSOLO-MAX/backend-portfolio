package dev.rainsolo.minirpc.demo;

import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.registry.FileServiceRegistry;
import dev.rainsolo.minirpc.server.RpcServer;

import java.nio.file.Path;

/** Run this first in one terminal; ConsumerMain runs in another JVM. */
public final class ProviderMain {

    public static void main(String[] args) throws Exception {
        FileServiceRegistry registry = new FileServiceRegistry(registryPath());
        RpcServer server = new RpcServer(9090, RpcConfig.defaults(), registry);
        server.registerService(DemoService.class, new DemoServiceImpl());
        server.start();
        System.out.println("Provider listening on 9090. Press Enter to stop.");
        System.in.read();
        server.close();
        registry.close();
    }

    public static Path registryPath() {
        return Path.of(System.getProperty("java.io.tmpdir"), "mini-rpc-registry");
    }
}
