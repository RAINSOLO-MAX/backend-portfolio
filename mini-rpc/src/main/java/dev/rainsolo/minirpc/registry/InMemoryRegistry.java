package dev.rainsolo.minirpc.registry;

import dev.rainsolo.minirpc.common.ServiceMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Thread-safe in-process registry, used by tests and single-JVM demos. */
public final class InMemoryRegistry implements ServiceRegistry {

    private final ConcurrentMap<String, Set<ServiceMeta>> services = new ConcurrentHashMap<>();
    private final long ttlMillis;

    public InMemoryRegistry() {
        this(30_000L);
    }

    public InMemoryRegistry(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    @Override
    public void register(ServiceMeta meta) {
        services.computeIfAbsent(meta.getServiceName(), ignored -> ConcurrentHashMap.newKeySet()).add(meta);
    }

    @Override
    public void deregister(ServiceMeta meta) {
        Set<ServiceMeta> instances = services.get(meta.getServiceName());
        if (instances != null) {
            instances.remove(meta);
        }
    }

    @Override
    public void heartbeat(ServiceMeta meta) {
        Set<ServiceMeta> instances = services.get(meta.getServiceName());
        if (instances != null) {
            instances.remove(meta);
            instances.add(meta.withHeartbeat(System.currentTimeMillis()));
        }
    }

    @Override
    public List<ServiceMeta> lookup(String serviceName) {
        Set<ServiceMeta> instances = services.get(serviceName);
        List<ServiceMeta> alive = new ArrayList<>();
        if (instances == null) {
            return alive;
        }
        long now = System.currentTimeMillis();
        for (ServiceMeta meta : instances) {
            if (now - meta.getLastSeenMillis() <= ttlMillis) {
                alive.add(meta);
            }
        }
        return alive;
    }

    @Override
    public void close() {
        services.clear();
    }
}
