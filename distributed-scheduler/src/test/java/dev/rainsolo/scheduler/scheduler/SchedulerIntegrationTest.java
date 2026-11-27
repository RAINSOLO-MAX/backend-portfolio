package dev.rainsolo.scheduler.scheduler;

import dev.rainsolo.scheduler.job.JobDefinition;
import dev.rainsolo.scheduler.job.JobListener;
import dev.rainsolo.scheduler.job.MisfirePolicy;
import dev.rainsolo.scheduler.job.ShardContext;
import dev.rainsolo.scheduler.sharding.AverageShardingStrategy;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerIntegrationTest {

    private static final long T0 = 1_704_067_200_000L;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final String JOB = "chain";

    @Test
    void manualClockDrivesFullChainAndEveryMetricIsCounted() {
        MutableClock clock = new MutableClock(T0);
        List<ShardContext> executed = new ArrayList<>();
        AtomicInteger transientFailures = new AtomicInteger();
        JobListener listener = ctx -> {
            executed.add(ctx);
            if (ctx.fireTimeMillis() == T0 + 50_000L && ctx.shardItem() == 0
                    && transientFailures.getAndIncrement() == 0) {
                throw new IllegalStateException("one transient failure");
            }
        };

        Scheduler scheduler = Scheduler.builder()
                .clock(clock)
                .zone(ZoneOffset.UTC)
                .shardingStrategy(new AverageShardingStrategy())
                .workerTimeoutNanos(TIMEOUT_NANOS)
                .maxAttempts(3)
                .build();
        scheduler.registerJob(JobDefinition.builder()
                .jobName(JOB)
                .cron("0/10 * * * * ?")
                .shardingTotalCount(2)
                .shardParameters(List.of("p0", "p1"))
                .misfirePolicy(MisfirePolicy.FIRE_ONCE_NOW)
                .timeoutMillis(5_000L)
                .failoverEnabled(true)
                .build(), listener);
        scheduler.workerRegistry().register("w1", clock.nanoTime());
        scheduler.workerRegistry().register("w2", clock.nanoTime());

        scheduler.tick();
        assertEquals(0L, scheduler.metrics().triggered());

        clock.advanceMillis(10_000L);
        scheduler.workerRegistry().heartbeat("w1", clock.nanoTime());
        scheduler.workerRegistry().heartbeat("w2", clock.nanoTime());
        scheduler.tick();

        clock.advanceMillis(10_000L);
        scheduler.workerRegistry().heartbeat("w1", clock.nanoTime());
        scheduler.tick();

        scheduler.lastFireStore().update(JOB, T0);
        scheduler.tick();

        clock.advanceMillis(30_000L);
        scheduler.workerRegistry().heartbeat("w1", clock.nanoTime());
        scheduler.tick();

        SchedulerMetrics.Snapshot snapshot = scheduler.metrics();
        assertEquals(4L, snapshot.triggered());
        assertEquals(7L, snapshot.shards());
        assertEquals(2L, snapshot.misfired());
        assertEquals(1L, snapshot.failover());
        assertEquals(2L, snapshot.duplicatesSuppressed());
        assertEquals(1L, snapshot.failed());
        assertEquals(1L, snapshot.retried());

        assertEquals(7, executed.size());
        assertEquals(6, scheduler.ledger().size());
        assertEquals(T0 + 50_000L, scheduler.lastFireStore().lastFire(JOB).longValue());
        assertEquals(ExecutionState.SUCCESS,
                scheduler.ledger().get(ExecutionRecord.key(JOB, T0 + 50_000L, 0)).state());
        assertEquals("p1", executed.stream()
                .filter(ctx -> ctx.fireTimeMillis() == T0 + 10_000L && ctx.shardItem() == 1)
                .findFirst().orElseThrow().shardParameter());
    }

    @Test
    void startAndStopControlBackgroundDriverWithoutBreakingManualTicks() {
        MutableClock clock = new MutableClock(T0);
        Scheduler scheduler = Scheduler.builder().clock(clock).zone(ZoneOffset.UTC).build();
        List<ShardContext> executed = new ArrayList<>();
        scheduler.registerJob(JobDefinition.builder()
                .jobName("driverJob").cron("0/10 * * * * ?")
                .shardingTotalCount(1).build(), executed::add);
        scheduler.workerRegistry().register("w1", clock.nanoTime());

        assertFalse(scheduler.coordinator().isRunning());
        scheduler.start();
        assertTrue(scheduler.coordinator().isRunning());
        scheduler.stop();
        assertFalse(scheduler.coordinator().isRunning());

        scheduler.tick();
        clock.advanceMillis(10_000L);
        scheduler.tick();
        assertEquals(1L, scheduler.metrics().triggered());
        assertEquals(1, executed.size());
    }
}
