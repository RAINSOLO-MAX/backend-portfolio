package dev.rainsolo.scheduler.sharding;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Round-robin assignment: shard items are spread evenly and contiguously over alive workers. */
public final class AverageShardingStrategy implements ShardingStrategy {

    @Override
    public Map<String, List<Integer>> assign(String jobName, List<String> aliveWorkers, int shardingTotalCount) {
        Map<String, List<Integer>> result = new LinkedHashMap<>();
        if (aliveWorkers == null || aliveWorkers.isEmpty() || shardingTotalCount <= 0) {
            return result;
        }
        for (String worker : aliveWorkers) {
            result.put(worker, new ArrayList<>());
        }
        for (int shard = 0; shard < shardingTotalCount; shard++) {
            String worker = aliveWorkers.get(shard % aliveWorkers.size());
            result.get(worker).add(shard);
        }
        return result;
    }
}
