package dev.rainsolo.minirpc.registry;

import dev.rainsolo.minirpc.common.ServiceMeta;

import java.util.List;

/** Provider registration and discovery. Stale instances are filtered on lookup. */
public interface ServiceRegistry extends AutoCloseable {

    void register(ServiceMeta meta);

    void deregister(ServiceMeta meta);

    /** Renew the lease of a live provider. */
    void heartbeat(ServiceMeta meta);

    List<ServiceMeta> lookup(String serviceName);

    @Override
    void close();
}
