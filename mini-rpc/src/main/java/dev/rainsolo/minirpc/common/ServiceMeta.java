package dev.rainsolo.minirpc.common;

import java.io.Serializable;
import java.util.Objects;

/** A provider instance registered in the service registry. */
public final class ServiceMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String serviceName;
    private final String host;
    private final int port;
    private final int weight;
    private final long lastSeenMillis;

    public ServiceMeta(String serviceName, String host, int port, int weight, long lastSeenMillis) {
        this.serviceName = Objects.requireNonNull(serviceName);
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.weight = Math.max(1, weight);
        this.lastSeenMillis = lastSeenMillis;
    }

    public ServiceMeta withHeartbeat(long now) {
        return new ServiceMeta(serviceName, host, port, weight, now);
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getWeight() {
        return weight;
    }

    public long getLastSeenMillis() {
        return lastSeenMillis;
    }

    public String endpoint() {
        return host + ":" + port;
    }

    /** Registry file key: one line per provider instance. */
    public String key() {
        return serviceName + "|" + endpoint();
    }

    public static ServiceMeta parse(String serviceName, String key, long lastSeenMillis, int weight) {
        int colon = key.lastIndexOf(':');
        return new ServiceMeta(serviceName, key.substring(0, colon),
                Integer.parseInt(key.substring(colon + 1)), weight, lastSeenMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ServiceMeta)) {
            return false;
        }
        ServiceMeta that = (ServiceMeta) other;
        return port == that.port && host.equals(that.host) && serviceName.equals(that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, host, port);
    }

    @Override
    public String toString() {
        return "ServiceMeta{" + key() + ", weight=" + weight + "}";
    }
}
