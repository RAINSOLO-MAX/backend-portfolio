package dev.rainsolo.minirpc.demo;

import dev.rainsolo.minirpc.client.RpcClient;
import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.registry.FileServiceRegistry;

/** Separate-JVM consumer: discovers the provider through the file-backed registry. */
public final class ConsumerMain {

    public static void main(String[] args) throws Exception {
        FileServiceRegistry registry = new FileServiceRegistry(ProviderMain.registryPath());
        for (int i = 0; i < 50 && registry.lookup(DemoService.class.getName()).isEmpty(); i++) {
            Thread.sleep(100);
        }
        try (RpcClient client = new RpcClient(RpcConfig.defaults(), registry)) {
            DemoService service = client.createProxy(DemoService.class);
            for (int i = 0; i < 5; i++) {
                System.out.println("call " + i + " -> " + service.echo("cross-jvm-" + i));
            }
        } finally {
            registry.close();
        }
    }
}
