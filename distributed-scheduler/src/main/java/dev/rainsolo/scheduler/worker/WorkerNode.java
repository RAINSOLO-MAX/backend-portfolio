package dev.rainsolo.scheduler.worker;

/** Immutable worker membership record; heartbeats are monotonic nanoseconds. */
public record WorkerNode(String workerId, long lastHeartbeatNanos) {
}
