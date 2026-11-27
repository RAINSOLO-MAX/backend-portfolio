package dev.rainsolo.scheduler.sharding;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Consistent hashing with 160 virtual nodes per worker. Adding or removing a worker only
 * remaps roughly 1/N of the shard items, and the full shard set is always preserved.
 */
public final class ConsistentHashShardingStrategy implements ShardingStrategy {

    /** Default number of virtual nodes per physical worker. */
    public static final int DEFAULT_VIRTUAL_NODES = 160;

    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final int virtualNodes;

    public ConsistentHashShardingStrategy() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashShardingStrategy(int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be positive");
        }
        this.virtualNodes = virtualNodes;
    }

    public int virtualNodes() {
        return virtualNodes;
    }

    @Override
    public Map<String, List<Integer>> assign(String jobName, List<String> aliveWorkers, int shardingTotalCount) {
        Map<String, List<Integer>> result = new LinkedHashMap<>();
        if (aliveWorkers == null || aliveWorkers.isEmpty() || shardingTotalCount <= 0) {
            return result;
        }
        TreeMap<Long, String> ring = new TreeMap<>();
        for (String worker : aliveWorkers) {
            for (int i = 0; i < virtualNodes; i++) {
                ring.put(hash(worker + "#" + i), worker);
            }
        }
        for (String worker : aliveWorkers) {
            result.put(worker, new ArrayList<>());
        }
        for (int shard = 0; shard < shardingTotalCount; shard++) {
            long point = hash(jobName + "#" + shard);
            Map.Entry<Long, String> entry = ring.ceilingEntry(point);
            if (entry == null) {
                entry = ring.firstEntry();
            }
            result.get(entry.getValue()).add(shard);
        }
        return result;
    }

    /** FNV-1a 64-bit hash followed by a Murmur-style avalanche mix, masked non-negative. */
    private static long hash(String key) {
        long h = FNV_OFFSET;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xffL);
            h *= FNV_PRIME;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h & Long.MAX_VALUE;
    }
}
