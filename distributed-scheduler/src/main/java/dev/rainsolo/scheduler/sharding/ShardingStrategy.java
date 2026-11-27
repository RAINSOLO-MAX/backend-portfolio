package dev.rainsolo.scheduler.sharding;

import java.util.List;
import java.util.Map;

/** Strategy that maps shard items (0-based) to alive worker ids for one job. */
public interface ShardingStrategy {

    /**
     * @param jobName            job name, used as part of the hash key
     * @param aliveWorkers       currently alive worker ids
     * @param shardingTotalCount number of shard items
     * @return assignment from worker id to the shard items it owns
     */
    Map<String, List<Integer>> assign(String jobName, List<String> aliveWorkers, int shardingTotalCount);
}
