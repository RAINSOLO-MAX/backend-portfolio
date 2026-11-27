package dev.rainsolo.scheduler.scheduler;

import dev.rainsolo.scheduler.sharding.ShardingStrategy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detects worker-list changes and reports shards that belonged to departed workers.
 * The new assignment is computed from the alive workers only; because executions are
 * keyed by {@code jobName#fireTimeMillis#shardItem}, re-running a reassigned shard
 * for the same fire time is suppressed by the ledger instead of being delivered twice.
 */
public final class FailoverRebalancer {

    /** The fresh assignment plus the orphan shards that had to be taken over. */
    public record Result(Map<String, List<Integer>> assignment, List<Integer> reassignedShards) {
    }

    private final Map<String, List<String>> lastWorkers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<Integer>>> lastAssignments = new ConcurrentHashMap<>();

    /** Recomputes the assignment and returns shards reclaimed from workers that disappeared. */
    public synchronized Result rebalance(String jobName, List<String> aliveWorkers,
                                         int shardingTotalCount, ShardingStrategy strategy) {
        Map<String, List<Integer>> assignment = strategy.assign(jobName, aliveWorkers, shardingTotalCount);
        List<String> previous = lastWorkers.get(jobName);
        List<Integer> reassigned = new ArrayList<>();
        if (previous != null) {
            Map<String, List<Integer>> old = lastAssignments.get(jobName);
            if (old != null) {
                for (String worker : previous) {
                    if (!aliveWorkers.contains(worker)) {
                        List<Integer> orphan = old.get(worker);
                        if (orphan != null) {
                            reassigned.addAll(orphan);
                        }
                    }
                }
            }
        }
        lastWorkers.put(jobName, List.copyOf(aliveWorkers));
        lastAssignments.put(jobName, copy(assignment));
        return new Result(assignment, List.copyOf(reassigned));
    }

    private static Map<String, List<Integer>> copy(Map<String, List<Integer>> source) {
        Map<String, List<Integer>> result = new LinkedHashMap<>();
        source.forEach((worker, shards) -> result.put(worker, List.copyOf(shards)));
        return result;
    }
}
