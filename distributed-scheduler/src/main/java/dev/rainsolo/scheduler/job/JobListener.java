package dev.rainsolo.scheduler.job;

/** Callback invoked once per shard of a triggered job. */
@FunctionalInterface
public interface JobListener {

    /** Runs the shard. A thrown exception marks the execution FAILED and triggers a retry. */
    void execute(ShardContext ctx) throws Exception;
}
