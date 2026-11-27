package dev.rainsolo.scheduler.sharding;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardingTest {

    @Test
    void averageStrategyCoversEveryShardExactlyOnce() {
        AverageShardingStrategy strategy = new AverageShardingStrategy();
        Map<String, List<Integer>> assignment = strategy.assign("orderSync",
                List.of("w1", "w2", "w3"), 10);
        assertEquals(3, assignment.size());
        Set<Integer> seen = new HashSet<>();
        for (List<Integer> shards : assignment.values()) {
            for (int shard : shards) {
                assertTrue(seen.add(shard), "shard must not be assigned twice");
            }
        }
        assertEquals(10, seen.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(seen.contains(i), "shard " + i + " must be assigned");
        }
        int min = assignment.values().stream().mapToInt(List::size).min().orElseThrow();
        int max = assignment.values().stream().mapToInt(List::size).max().orElseThrow();
        assertTrue(max - min <= 1, "average strategy must be balanced");
        assertTrue(strategy.assign("job", List.of(), 5).isEmpty());
    }

    @Test
    void consistentHashRemapsOnlyAFractionWhenAWorkerJoins() {
        ConsistentHashShardingStrategy strategy = new ConsistentHashShardingStrategy();
        assertEquals(ConsistentHashShardingStrategy.DEFAULT_VIRTUAL_NODES, strategy.virtualNodes());

        int total = 300;
        List<String> three = List.of("w1", "w2", "w3");
        List<String> four = List.of("w1", "w2", "w3", "w4");
        Map<String, List<Integer>> before = strategy.assign("orderSync", three, total);
        Map<String, List<Integer>> after = strategy.assign("orderSync", four, total);

        Map<Integer, String> ownerBefore = invert(before);
        Map<Integer, String> ownerAfter = invert(after);
        assertEquals(total, ownerBefore.size());
        assertEquals(total, ownerAfter.size());
        for (int shard = 0; shard < total; shard++) {
            assertTrue(ownerAfter.containsKey(shard), "shard set must stay complete");
        }

        int remapped = 0;
        for (int shard = 0; shard < total; shard++) {
            if (!ownerBefore.get(shard).equals(ownerAfter.get(shard))) {
                remapped++;
            }
        }
        assertTrue(remapped > 0, "adding a worker must take over some shards");
        assertTrue(remapped < total * 0.5,
                "consistent hash must remap far less than the full shard set, got " + remapped);
        assertTrue(strategy.assign("job", List.of(), total).isEmpty());
    }

    @Test
    void consistentHashCustomVirtualNodeCountKeepsShardsComplete() {
        ConsistentHashShardingStrategy strategy = new ConsistentHashShardingStrategy(40);
        Map<String, List<Integer>> assignment = strategy.assign("report", List.of("a", "b"), 25);
        List<Integer> all = new ArrayList<>();
        assignment.values().forEach(all::addAll);
        all.sort(Integer::compareTo);
        assertEquals(25, all.size());
        for (int i = 0; i < 25; i++) {
            assertEquals(i, all.get(i));
        }
    }

    private static Map<Integer, String> invert(Map<String, List<Integer>> assignment) {
        Map<Integer, String> owners = new HashMap<>();
        assignment.forEach((worker, shards) -> shards.forEach(shard -> owners.put(shard, worker)));
        return owners;
    }
}
