package dev.rainsolo.minirpc.loadbalance;

import dev.rainsolo.minirpc.common.ServiceMeta;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class RoundRobinLoadBalance implements LoadBalance {

    private final ConcurrentMap<String, AtomicInteger> cursors = new ConcurrentHashMap<>();

    @Override
    public ServiceMeta select(String serviceName, List<ServiceMeta> providers, String requestKey) {
        AtomicInteger cursor = cursors.computeIfAbsent(serviceName, ignored -> new AtomicInteger());
        int index = Math.floorMod(cursor.getAndIncrement(), providers.size());
        return providers.get(index);
    }
}
