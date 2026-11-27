package dev.rainsolo.minirpc.loadbalance;

import dev.rainsolo.minirpc.common.ServiceMeta;

import java.util.List;

public interface LoadBalance {

    /** @param requestKey used by hash-based strategies to keep a caller sticky to one provider */
    ServiceMeta select(String serviceName, List<ServiceMeta> providers, String requestKey);
}
