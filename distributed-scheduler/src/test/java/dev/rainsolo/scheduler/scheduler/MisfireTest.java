package dev.rainsolo.scheduler.scheduler;

import dev.rainsolo.scheduler.job.JobDefinition;
import dev.rainsolo.scheduler.job.MisfirePolicy;
import dev.rainsolo.scheduler.job.ShardContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MisfireTest {

    private static final long T0 = 1_704_067_200_000L;
    private static final String JOB = "misfireJob";

    private Scheduler newScheduler(MutableClock clock, MisfirePolicy policy, List<ShardContext> executed) {
        Scheduler scheduler = Scheduler.builder()
                .clock(clock)
                .zone(ZoneOffset.UTC)
                .workerTimeoutNanos(Long.MAX_VALUE)
                .maxAttempts(3)
                .build();
        scheduler.registerJob(JobDefinition.builder()
                .jobName(JOB)
                .cron("0/10 * * * * ?")
                .shardingTotalCount(2)
                .misfirePolicy(policy)
                .build(), executed::add);
        scheduler.workerRegistry().register("w1", clock.nanoTime());
        return scheduler;
    }

    @Test
    void fireOnceNowCompensatesExactlyOneOfThreeMissedCycles() {
        MutableClock clock = new MutableClock(T0);
        List<ShardContext> executed = new ArrayList<>();
        Scheduler scheduler = newScheduler(clock, MisfirePolicy.FIRE_ONCE_NOW, executed);

        scheduler.tick();
        clock.advanceMillis(10_000L);
        executed.clear();
        scheduler.tick();
        assertEquals(2, executed.size(), "first cycle is a normal two-shard fire");
        assertEquals(T0 + 10_000L, scheduler.lastFireStore().lastFire(JOB).longValue());

        clock.advanceMillis(30_000L);
        executed.clear();
        scheduler.tick();
        assertEquals(2, executed.size(), "FIRE_ONCE_NOW must compensate exactly one cycle");
        assertEquals(T0 + 40_000L, executed.get(0).fireTimeMillis());
        assertEquals(1L, scheduler.metrics().misfired());
        assertEquals(T0 + 40_000L, scheduler.lastFireStore().lastFire(JOB).longValue());
    }

    @Test
    void ignoreCompensatesNothingButAdvancesLastFire() {
        MutableClock clock = new MutableClock(T0);
        List<ShardContext> executed = new ArrayList<>();
        Scheduler scheduler = newScheduler(clock, MisfirePolicy.IGNORE, executed);

        scheduler.tick();
        clock.advanceMillis(10_000L);
        scheduler.tick();
        executed.clear();
        clock.advanceMillis(30_000L);
        scheduler.tick();

        assertTrue(executed.isEmpty(), "IGNORE must not compensate any missed cycle");
        assertEquals(0L, scheduler.metrics().misfired());
        assertEquals(T0 + 40_000L, scheduler.lastFireStore().lastFire(JOB).longValue());
    }

    @Test
    void rescheduleNextCompensatesNothingButAdvancesLastFire() {
        MutableClock clock = new MutableClock(T0);
        List<ShardContext> executed = new ArrayList<>();
        Scheduler scheduler = newScheduler(clock, MisfirePolicy.RESCHEDULE_NEXT, executed);

        scheduler.tick();
        clock.advanceMillis(10_000L);
        scheduler.tick();
        executed.clear();
        clock.advanceMillis(30_000L);
        scheduler.tick();

        assertTrue(executed.isEmpty());
        assertEquals(0L, scheduler.metrics().misfired());
        assertEquals(T0 + 40_000L, scheduler.lastFireStore().lastFire(JOB).longValue());
    }

    @Test
    void lastFireStorePersistsAndReloads(@TempDir Path dir) {
        Path file = dir.resolve("lastfire.properties");
        LastFireStore first = new LastFireStore(file);
        first.update("alpha", 111L);
        first.update("beta", 222L);
        first.snapshot();

        LastFireStore reloaded = new LastFireStore(file);
        assertEquals(111L, reloaded.lastFire("alpha").longValue());
        assertEquals(222L, reloaded.lastFire("beta").longValue());
        assertEquals(2, reloaded.size());

        reloaded.update("alpha", 333L);
        assertEquals(333L, new LastFireStore(file).lastFire("alpha").longValue());
        assertNull(new LastFireStore(dir.resolve("missing.properties")).lastFire("alpha"));
        assertEquals(0, LastFireStore.inMemory().size());
    }

    @Test
    void restartedSchedulerRecoversLastFireFromDiskAndCompensates(@TempDir Path dir) {
        Path file = dir.resolve("restart.properties");
        MutableClock clock = new MutableClock(T0);
        List<ShardContext> executed = new ArrayList<>();
        Scheduler original = Scheduler.builder()
                .clock(clock).zone(ZoneOffset.UTC).lastFirePath(file)
                .workerTimeoutNanos(Long.MAX_VALUE).maxAttempts(3).build();
        original.registerJob(JobDefinition.builder()
                .jobName(JOB).cron("0/10 * * * * ?").shardingTotalCount(2)
                .misfirePolicy(MisfirePolicy.FIRE_ONCE_NOW).build(), executed::add);
        original.workerRegistry().register("w1", clock.nanoTime());
        original.tick();
        clock.advanceMillis(10_000L);
        original.tick();

        clock.advanceMillis(30_000L);
        Scheduler restarted = Scheduler.builder()
                .clock(clock).zone(ZoneOffset.UTC).lastFirePath(file)
                .workerTimeoutNanos(Long.MAX_VALUE).maxAttempts(3).build();
        assertEquals(T0 + 10_000L, restarted.lastFireStore().lastFire(JOB).longValue());
        restarted.registerJob(JobDefinition.builder()
                .jobName(JOB).cron("0/10 * * * * ?").shardingTotalCount(2)
                .misfirePolicy(MisfirePolicy.FIRE_ONCE_NOW).build(), executed::add);
        restarted.workerRegistry().register("w1", clock.nanoTime());
        executed.clear();
        restarted.tick();

        assertEquals(2, executed.size(), "reloaded scheduler compensates exactly once");
        assertEquals(1L, restarted.metrics().misfired());
        assertEquals(T0 + 40_000L, restarted.lastFireStore().lastFire(JOB).longValue());
    }
}
