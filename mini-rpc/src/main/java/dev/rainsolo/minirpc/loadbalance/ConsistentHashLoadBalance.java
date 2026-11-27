package dev.rainsolo.minirpc.loadbalance;

import dev.rainsolo.minirpc.common.ServiceMeta;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/** Consistent hashing with virtual nodes; adding/removing a provider remaps only ~1/N keys. */
public final class ConsistentHashLoadBalance implements LoadBalance {

    private final int virtualNodes;
    private final ConcurrentMap<String, Ring> rings = new ConcurrentHashMap<>();

    public ConsistentHashLoadBalance() {
        this(160);
    }

    public ConsistentHashLoadBalance(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    @Override
    public ServiceMeta select(String serviceName, List<ServiceMeta> providers, String requestKey) {
        Ring ring = rings.compute(serviceName, (key, old) ->
                old != null && old.matches(providers) ? old : new Ring(providers, virtualNodes));
        return ring.select(requestKey == null ? serviceName : requestKey);
    }

    private static long hash(String text) {
        long value = 0xcbf29ce484222325L;
        for (byte b : text.getBytes(StandardCharsets.UTF_8)) {
            value ^= b;
            value *= 0x100000001b3L;
        }
        return value;
    }

    private static final class Ring {

        private final String signature;
        private final TreeMap<Long, ServiceMeta> circle = new TreeMap<>();

        Ring(List<ServiceMeta> providers, int virtualNodes) {
            this.signature = providers.stream().map(ServiceMeta::endpoint).sorted().collect(Collectors.joining(","));
            for (ServiceMeta provider : providers) {
                for (int i = 0; i < virtualNodes; i++) {
                    circle.put(hash(provider.endpoint() + "#" + i), provider);
                }
            }
        }

        boolean matches(List<ServiceMeta> providers) {
            String current = providers.stream().map(ServiceMeta::endpoint).sorted().collect(Collectors.joining(","));
            return signature.equals(current);
        }

        ServiceMeta select(String key) {
            if (circle.isEmpty()) {
                return null;
            }
            Map.Entry<Long, ServiceMeta> entry = circle.ceilingEntry(hash(key));
            return entry == null ? circle.firstEntry().getValue() : entry.getValue();
        }
    }
}
