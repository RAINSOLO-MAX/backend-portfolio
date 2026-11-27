package dev.rainsolo.delayqueue.core;

/** Immutable identity, mutable scheduling state. Deadlines are monotonic nanoseconds. */
public final class DelayedMessage {

    private final String id;
    private final String topic;
    private final String payload;
    private final int maxAttempts;
    private final long createdWallMillis;

    private volatile long deadlineNanos;
    private volatile int attempts;
    private volatile MessageStatus status;

    public DelayedMessage(String id, String topic, String payload, long deadlineNanos,
                          int maxAttempts, long createdWallMillis) {
        this.id = id;
        this.topic = topic;
        this.payload = payload;
        this.deadlineNanos = deadlineNanos;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.createdWallMillis = createdWallMillis;
        this.status = MessageStatus.PENDING;
    }

    public String getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public long getDeadlineNanos() {
        return deadlineNanos;
    }

    public void setDeadlineNanos(long value) {
        this.deadlineNanos = value;
    }

    public int getAttempts() {
        return attempts;
    }

    public synchronized int incrementAttempts() {
        return ++attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getCreatedWallMillis() {
        return createdWallMillis;
    }

    public MessageStatus getStatus() {
        return status;
    }

    public void setStatus(MessageStatus value) {
        this.status = value;
    }

    @Override
    public String toString() {
        return "DelayedMessage{" + id + ", topic=" + topic + ", attempts=" + attempts
                + "/" + maxAttempts + ", status=" + status + "}";
    }
}
