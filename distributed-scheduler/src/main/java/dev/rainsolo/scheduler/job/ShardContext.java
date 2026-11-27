package dev.rainsolo.scheduler.job;

/** Immutable context handed to a job listener for one shard execution. */
public record ShardContext(String jobName, long fireTimeMillis, int shardItem, String shardParameter) {
}
