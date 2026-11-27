package dev.rainsolo.scheduler.demo;

import dev.rainsolo.scheduler.job.JobDefinition;
import dev.rainsolo.scheduler.job.JobListener;
import dev.rainsolo.scheduler.job.MisfirePolicy;
import dev.rainsolo.scheduler.scheduler.Scheduler;
import dev.rainsolo.scheduler.sharding.ConsistentHashShardingStrategy;
import dev.rainsolo.scheduler.sharding.ShardingStrategy;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Throughput and p99 latency benchmark driven by a deterministic clock. */
public final class SchedulerBenchmark {

    private static final int JOBS = 20;
    private static final int TRIGGERS = 200;
    private static final int SHARDS = 8;
    private static final long STEP_MILLIS = 10_000L;
    private static final long START_MILLIS = 1_704_067_200_000L;
    private static final List<String> WORKERS = List.of("w1", "w2", "w3", "w4");

    public static void main(String[] args) {
        ManualClock clock = new ManualClock(START_MILLIS);
        ShardingStrategy strategy = new ConsistentHashShardingStrategy();
        Scheduler scheduler = Scheduler.builder()
                .clock(clock)
                .zone(ZoneOffset.UTC)
                .shardingStrategy(strategy)
                .workerTimeoutNanos(Long.MAX_VALUE)
                .maxAttempts(3)
                .build();

        AtomicLong executed = new AtomicLong();
        JobListener listener = ctx -> executed.incrementAndGet();
        for (int i = 0; i < JOBS; i++) {
            scheduler.registerJob(JobDefinition.builder()
                    .jobName("bench-" + i)
                    .cron("0/10 * * * * ?")
                    .shardingTotalCount(SHARDS)
                    .misfirePolicy(MisfirePolicy.IGNORE)
                    .build(), listener);
        }
        for (String worker : WORKERS) {
            scheduler.workerRegistry().register(worker, clock.nanoTime());
        }
        scheduler.tick();

        long[] tickCosts = new long[TRIGGERS];
        long[] assignCosts = new long[TRIGGERS * JOBS];
        long wallStart = System.nanoTime();
        for (int t = 0; t < TRIGGERS; t++) {
            clock.advanceMillis(STEP_MILLIS);
            for (int j = 0; j < JOBS; j++) {
                long a0 = System.nanoTime();
                strategy.assign("bench-" + j, WORKERS, SHARDS);
                assignCosts[t * JOBS + j] = System.nanoTime() - a0;
            }
            long t0 = System.nanoTime();
            scheduler.tick();
            tickCosts[t] = System.nanoTime() - t0;
        }
        long wallElapsed = System.nanoTime() - wallStart;

        long tickTotal = 0L;
        for (long cost : tickCosts) {
            tickTotal += cost;
        }
        long triggered = scheduler.metrics().triggered();
        double throughput = tickTotal == 0L ? 0.0 : triggered * 1_000_000_000.0 / tickTotal;
        Arrays.sort(tickCosts);
        Arrays.sort(assignCosts);

        System.out.println("=== distributed-scheduler benchmark ===");
        System.out.println("jobs=" + JOBS + " triggersPerJob=" + TRIGGERS
                + " shardsPerJob=" + SHARDS + " workers=" + WORKERS.size());
        System.out.println("triggered=" + triggered + " shardExecutions=" + executed.get());
        System.out.println("wallElapsedMillis=" + wallElapsed / 1_000_000L);
        System.out.printf("throughput=%.2f triggers/s%n", throughput);
        System.out.println("tickP99Nanos=" + percentile(tickCosts));
        System.out.println("assignP99Nanos=" + percentile(assignCosts));
        System.out.println("=== benchmark finished ===");
    }

    private static long percentile(long[] sorted) {
        int index = (int) Math.ceil(0.99 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
    }
}
