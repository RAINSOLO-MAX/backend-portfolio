package dev.rainsolo.scheduler.scheduler;

import dev.rainsolo.scheduler.job.ShardContext;

/** One shard execution, uniquely identified by {@code jobName#fireTimeMillis#shardItem}. */
public final class ExecutionRecord {

    private final String key;
    private final String jobName;
    private final long fireTimeMillis;
    private final int shardItem;
    private final ShardContext context;

    private volatile ExecutionState state;
    private volatile int attempts;

    public ExecutionRecord(String jobName, long fireTimeMillis, int shardItem, ShardContext context) {
        this.jobName = jobName;
        this.fireTimeMillis = fireTimeMillis;
        this.shardItem = shardItem;
        this.context = context;
        this.key = key(jobName, fireTimeMillis, shardItem);
        this.state = ExecutionState.PENDING;
        this.attempts = 0;
    }

    /** Builds the idempotency key used by the ledger. */
    public static String key(String jobName, long fireTimeMillis, int shardItem) {
        return jobName + "#" + fireTimeMillis + "#" + shardItem;
    }

    public String key() {
        return key;
    }

    public String jobName() {
        return jobName;
    }

    public long fireTimeMillis() {
        return fireTimeMillis;
    }

    public int shardItem() {
        return shardItem;
    }

    public ShardContext context() {
        return context;
    }

    public ExecutionState state() {
        return state;
    }

    public void setState(ExecutionState value) {
        this.state = value;
    }

    public int attempts() {
        return attempts;
    }

    public synchronized void incrementAttempts() {
        attempts++;
    }
}
