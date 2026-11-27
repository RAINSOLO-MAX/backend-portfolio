package dev.rainsolo.minirpc;

import dev.rainsolo.minirpc.common.ServiceMeta;
import dev.rainsolo.minirpc.loadbalance.ConsistentHashLoadBalance;
import dev.rainsolo.minirpc.loadbalance.RandomLoadBalance;
import dev.rainsolo.minirpc.loadbalance.RoundRobinLoadBalance;
import dev.rainsolo.minirpc.registry.FileServiceRegistry;
import dev.rainsolo.minirpc.registry.InMemoryRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryAndLoadBalanceTest {

    private static final String SERVICE = "dev.rainsolo.EchoService";

    private static ServiceMeta provider(int port) {
        return new ServiceMeta(SERVICE, "127.0.0.1", port, 1, System.currentTimeMillis());
    }

    @Test
    void staleInstancesExpireAndHeartbeatRenews() throws InterruptedException {
        InMemoryRegistry registry = new InMemoryRegistry(60);
        ServiceMeta meta = provider(9001);
        registry.register(meta);
        assertEquals(1, registry.lookup(SERVICE).size());

        Thread.sleep(90);
        assertEquals(0, registry.lookup(SERVICE).size());

        registry.heartbeat(meta);
        assertEquals(1, registry.lookup(SERVICE).size());
        registry.close();
    }

    @Test
    void fileRegistrySurvivesAcrossRegistryInstances(@TempDir Path directory) {
        try (FileServiceRegistry writer = new FileServiceRegistry(directory);
             FileServiceRegistry reader = new FileServiceRegistry(directory)) {
            ServiceMeta meta = provider(9002);
            writer.register(meta);
            assertEquals(1, reader.lookup(SERVICE).size());
            assertEquals("127.0.0.1:9002", reader.lookup(SERVICE).get(0).endpoint());
            writer.deregister(meta);
            assertEquals(0, reader.lookup(SERVICE).size());
        }
    }

    @Test
    void roundRobinCyclesThroughProviders() {
        List<ServiceMeta> providers = List.of(provider(1), provider(2), provider(3));
        RoundRobinLoadBalance balance = new RoundRobinLoadBalance();
        List<Integer> ports = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ports.add(balance.select(SERVICE, providers, "k").getPort());
        }
        assertEquals(List.of(1, 2, 3, 1, 2, 3), ports);
    }

    @Test
    void weightedRandomFavoursHeavierProvider() {
        List<ServiceMeta> providers = List.of(
                new ServiceMeta(SERVICE, "127.0.0.1", 1, 1, System.currentTimeMillis()),
                new ServiceMeta(SERVICE, "127.0.0.1", 2, 9, System.currentTimeMillis()));
        RandomLoadBalance balance = new RandomLoadBalance();
        int heavy = 0;
        for (int i = 0; i < 2_000; i++) {
            if (balance.select(SERVICE, providers, "k").getPort() == 2) {
                heavy++;
            }
        }
        assertTrue(heavy > 1_600, "heavy node should receive roughly 90% of traffic, got " + heavy);
    }

    @Test
    void consistentHashIsStableAndOnlyRemapsAFractionWhenNodesChange() {
        List<ServiceMeta> two = List.of(provider(1), provider(2));
        List<ServiceMeta> three = List.of(provider(1), provider(2), provider(3));
        ConsistentHashLoadBalance balance = new ConsistentHashLoadBalance();
        int remapped = 0;
        for (int i = 0; i < 500; i++) {
            String key = "user-" + i;
            int before = balance.select(SERVICE, two, key).getPort();
            int after = balance.select(SERVICE, three, key).getPort();
            if (before != after) {
                remapped++;
            }
        }
        assertTrue(remapped > 0 && remapped < 250,
                "adding one node should remap only a fraction of keys, got " + remapped);
    }
}
