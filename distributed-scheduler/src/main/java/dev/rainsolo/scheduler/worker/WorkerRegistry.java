package dev.rainsolo.scheduler.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe membership registry that evicts workers whose heartbeat is older than a timeout. */
public final class WorkerRegistry {

    private final ConcurrentHashMap<String, WorkerNode> nodes = new ConcurrentHashMap<>();

    /** Registers (or refreshes) a worker with a caller-supplied monotonic timestamp. */
    public void register(String workerId, long nowNanos) {
        nodes.put(workerId, new WorkerNode(workerId, nowNanos));
    }

    /** Registers a worker using the JVM monotonic clock. */
    public void register(String workerId) {
        register(workerId, System.nanoTime());
    }

    /** Refreshes the heartbeat of an existing worker; unknown ids are ignored. */
    public void heartbeat(String workerId, long nowNanos) {
        nodes.computeIfPresent(workerId, (id, node) -> new WorkerNode(id, nowNanos));
    }

    /** Refreshes the heartbeat using the JVM monotonic clock. */
    public void heartbeat(String workerId) {
        heartbeat(workerId, System.nanoTime());
    }

    /** Explicitly removes a worker. */
    public boolean remove(String workerId) {
        return nodes.remove(workerId) != null;
    }

    public boolean contains(String workerId) {
        return nodes.containsKey(workerId);
    }

    public int size() {
        return nodes.size();
    }

    /**
     * Evicts workers with {@code nowNanos - lastHeartbeatNanos > timeoutNanos} and returns
     * the remaining ids in sorted order, so assignments are deterministic.
     */
    public List<String> aliveWorkers(long nowNanos, long timeoutNanos) {
        nodes.entrySet().removeIf(entry -> nowNanos - entry.getValue().lastHeartbeatNanos() > timeoutNanos);
        List<String> ids = new ArrayList<>(nodes.keySet());
        Collections.sort(ids);
        return List.copyOf(ids);
    }

    /** Convenience overload using the JVM monotonic clock. */
    public List<String> aliveWorkers(long timeoutNanos) {
        return aliveWorkers(System.nanoTime(), timeoutNanos);
    }

    /** Returns a snapshot of all registered nodes. */
    public List<WorkerNode> nodes() {
        return List.copyOf(nodes.values());
    }
}
