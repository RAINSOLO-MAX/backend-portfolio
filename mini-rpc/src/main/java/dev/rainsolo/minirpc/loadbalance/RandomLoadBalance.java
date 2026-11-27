package dev.rainsolo.minirpc.loadbalance;

import dev.rainsolo.minirpc.common.ServiceMeta;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** Weighted random selection; weight comes from the registry entry. */
public final class RandomLoadBalance implements LoadBalance {

    @Override
    public ServiceMeta select(String serviceName, List<ServiceMeta> providers, String requestKey) {
        int total = 0;
        for (ServiceMeta provider : providers) {
            total += provider.getWeight();
        }
        int point = ThreadLocalRandom.current().nextInt(total);
        for (ServiceMeta provider : providers) {
            point -= provider.getWeight();
            if (point < 0) {
                return provider;
            }
        }
        return providers.get(providers.size() - 1);
    }
}
