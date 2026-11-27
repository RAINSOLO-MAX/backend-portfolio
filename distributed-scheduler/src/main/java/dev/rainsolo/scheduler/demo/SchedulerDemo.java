package dev.rainsolo.scheduler.demo;

import dev.rainsolo.scheduler.job.JobDefinition;
import dev.rainsolo.scheduler.job.JobListener;
import dev.rainsolo.scheduler.job.MisfirePolicy;
import dev.rainsolo.scheduler.job.ShardContext;
import dev.rainsolo.scheduler.scheduler.LastFireStore;
import dev.rainsolo.scheduler.scheduler.Scheduler;
import dev.rainsolo.scheduler.sharding.ConsistentHashShardingStrategy;
import dev.rainsolo.scheduler.sharding.ShardingStrategy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** End-to-end demo: normal triggers, worker failover and snapshot-based misfire recovery. */
public final class SchedulerDemo {

    private static final long START_MILLIS = 1_704_067_200_000L;
    private static final long STEP_MILLIS = 10_000L;
    private static final long WORKER_TIMEOUT_NANOS = 30_000_000_000L;
    private static final String JOB = "orderSync";
    private static final int SHARDS = 5;
    private static final List<String> WORKERS = List.of("worker-A", "worker-B", "worker-C");

    public static void main(String[] args) throws Exception {
        Path snapshot = Files.createTempFile("scheduler-demo-", ".properties");
        snapshot.toFile().deleteOnExit();

        ManualClock clock = new ManualClock(START_MILLIS);
        ShardingStrategy strategy = new ConsistentHashShardingStrategy();
        Scheduler scheduler = build(clock, strategy, snapshot);
        List<ShardContext> executed = new ArrayList<>();
        JobListener listener = executed::add;
        scheduler.registerJob(job(), listener);
        for (String worker : WORKERS) {
            scheduler.workerRegistry().register(worker, clock.nanoTime());
        }

        System.out.println("=== distributed-scheduler demo (manual clock) ===");
        System.out.println("job=" + JOB + " cron=0/10 * * * * ? shards=" + SHARDS
                + " misfire=FIRE_ONCE_NOW strategy=consistent-hash");

        System.out.println("-- phase 1: normal triggers --");
        scheduler.tick();
        for (int i = 1; i <= 3; i++) {
            clock.advanceMillis(STEP_MILLIS);
            keepAlive(scheduler, clock);
            executed.clear();
            scheduler.tick();
            printFire("normal-" + i, scheduler, strategy, executed, clock);
        }

        System.out.println("-- phase 2: worker-C heartbeat timeout, failover rebalance --");
        clock.advanceMillis(31_000L);
        scheduler.workerRegistry().heartbeat("worker-A", clock.nanoTime());
        scheduler.workerRegistry().heartbeat("worker-B", clock.nanoTime());
        executed.clear();
        scheduler.tick();
        List<String> alive = scheduler.workerRegistry().aliveWorkers(clock.nanoTime(), WORKER_TIMEOUT_NANOS);
        System.out.println("aliveWorkers=" + alive + " departed=[worker-C]");
        System.out.println("previousAssignment=" + strategy.assign(JOB, WORKERS, SHARDS));
        System.out.println("currentAssignment=" + strategy.assign(JOB, alive, SHARDS));
        printFire("failover", scheduler, strategy, executed, clock);
        System.out.println("failoverReassigned=" + scheduler.metrics().failover()
                + " misfired=" + scheduler.metrics().misfired());

        System.out.println("-- phase 3: snapshot, downtime, reload, misfire compensation --");
        scheduler.lastFireStore().snapshot();
        LastFireStore reloaded = new LastFireStore(snapshot);
        System.out.println("snapshotWritten=" + snapshot.getFileName()
                + " reloadedLastFire=" + reloaded.lastFire(JOB));
        clock.advanceMillis(3 * STEP_MILLIS);
        Scheduler restarted = build(clock, strategy, snapshot);
        restarted.registerJob(job(), executed::add);
        for (String worker : WORKERS) {
            restarted.workerRegistry().register(worker, clock.nanoTime());
        }
        executed.clear();
        restarted.tick();
        printFire("after-reload", restarted, strategy, executed, clock);
        System.out.println("misfiredAfterReload=" + restarted.metrics().misfired()
                + " triggered=" + restarted.metrics().triggered()
                + " shards=" + restarted.metrics().shards());
        System.out.println("=== demo finished ===");
    }

    private static Scheduler build(ManualClock clock, ShardingStrategy strategy, Path snapshot) {
        return Scheduler.builder()
                .clock(clock)
                .zone(ZoneOffset.UTC)
                .lastFirePath(snapshot)
                .shardingStrategy(strategy)
                .workerTimeoutNanos(WORKER_TIMEOUT_NANOS)
                .maxAttempts(3)
                .build();
    }

    private static JobDefinition job() {
        return JobDefinition.builder()
                .jobName(JOB)
                .cron("0/10 * * * * ?")
                .shardingTotalCount(SHARDS)
                .shardParameters(List.of("alpha", "beta", "gamma", "delta", "epsilon"))
                .misfirePolicy(MisfirePolicy.FIRE_ONCE_NOW)
                .timeoutMillis(5_000L)
                .failoverEnabled(true)
                .build();
    }

    private static void keepAlive(Scheduler scheduler, ManualClock clock) {
        for (String worker : WORKERS) {
            scheduler.workerRegistry().heartbeat(worker, clock.nanoTime());
        }
    }

    private static void printFire(String phase, Scheduler scheduler, ShardingStrategy strategy,
                                  List<ShardContext> executed, ManualClock clock) {
        if (executed.isEmpty()) {
            System.out.println(phase + " fire=none");
            return;
        }
        long fireTime = executed.get(0).fireTimeMillis();
        List<Integer> items = executed.stream()
                .map(ShardContext::shardItem)
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
        List<String> alive = scheduler.workerRegistry().aliveWorkers(clock.nanoTime(), WORKER_TIMEOUT_NANOS);
        Map<String, List<Integer>> assignment = strategy.assign(JOB, alive, SHARDS);
        System.out.println(phase + " fireTime=" + fireTime + " executedShards=" + items
                + " assignment=" + assignment);
    }
}
