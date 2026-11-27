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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailoverAndIdempotencyTest {

    private static final long T0 = 1_704_067_200_000L;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final String JOB = "failoverJob";

    private Scheduler build(MutableClock clock, MisfirePolicy policy, JobListener listener) {
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
                .shardingTotalCount(4)
                .misfirePolicy(policy)
                .failoverEnabled(true)
                .build(), listener);
        scheduler.workerRegistry().register("w1", clock.nanoTime());
        scheduler.workerRegistry().register("w2", clock.nanoTime());
        return scheduler;
    }

    @Test
    void departedWorkerShardsAreReassignedAndStillExecuted() {
        MutableClock clock = new MutableClock(T0);
        List<ShardContext> executed = new ArrayList<>();
        Scheduler scheduler = build(clock, MisfirePolicy.IGNORE, executed::add);

        scheduler.tick();
        clock.advanceMillis(10_000L);
        scheduler.workerRegistry().heartbeat("w1", clock.nanoTime());
        scheduler.workerRegistry().heartbeat("w2", clock.nanoTime());
        executed.clear();
        scheduler.tick();
        assertEquals(4, executed.size(), "both workers should cover all four shards");
        assertEquals(0L, scheduler.metrics().failover());

        clock.advanceMillis(10_000L);
        scheduler.workerRegistry().heartbeat("w1", clock.nanoTime());
        executed.clear();
        scheduler.tick();

        assertEquals(List.of("w1"), scheduler.workerRegistry().aliveWorkers(clock.nanoTime(), TIMEOUT_NANOS));
        Set<Integer> items = executed.stream()
                .map(ShardContext::shardItem)
                .collect(Collectors.toSet());
        assertEquals(Set.of(0, 1, 2, 3), items, "departed worker shards must still run");
        assertEquals(4, executed.size());
        assertEquals(2L, scheduler.metrics().failover(), "w2 owned shards 1 and 3");
    }

    @Test
    void duplicateFireTimeAndShardIsSuppressedByLedger() {
        ExecutionLedger ledger = new ExecutionLedger(3);
        ShardContext context = new ShardContext(JOB, T0 + 10_000L, 0, "p0");
        ExecutionRecord first = new ExecutionRecord(JOB, T0 + 10_000L, 0, context);
        ExecutionRecord claimed = ledger.tryClaim(first);
        assertNotNull(claimed);
        ledger.markSuccess(claimed);
        assertNull(ledger.tryClaim(new ExecutionRecord(JOB, T0 + 10_000L, 0, context)));
        assertEquals(1L, ledger.duplicatesSuppressed());
        assertEquals(1, ledger.size());
        assertEquals(ExecutionRecord.key(JOB, T0 + 10_000L, 0), first.key());

        MutableClock clock = new MutableClock(T0);
        List<ShardContext> executed = new ArrayList<>();
        Scheduler scheduler = build(clock, MisfirePolicy.IGNORE, executed::add);
        scheduler.tick();
        clock.advanceMillis(10_000L);
        scheduler.workerRegistry().heartbeat("w1", clock.nanoTime());
        scheduler.workerRegistry().heartbeat("w2", clock.nanoTime());
        scheduler.tick();
        long shardsBefore = scheduler.metrics().shards();

        scheduler.lastFireStore().update(JOB, T0);
        executed.clear();
        scheduler.tick();

        assertTrue(executed.isEmpty(), "replaying the same fire time must not re-run listeners");
        assertEquals(4L, scheduler.metrics().duplicatesSuppressed());
        assertEquals(shardsBefore, scheduler.metrics().shards());
    }

    @Test
    void failedListenerRetriesUntilSuccess() {
        MutableClock clock = new MutableClock(T0);
        AtomicInteger calls = new AtomicInteger();
        JobListener flaky = ctx -> {
            if (calls.incrementAndGet() <= 2) {
                throw new IllegalStateException("transient failure");
            }
        };
        Scheduler scheduler = Scheduler.builder()
                .clock(clock).zone(ZoneOffset.UTC)
                .workerTimeoutNanos(Long.MAX_VALUE).maxAttempts(3).build();
        scheduler.registerJob(JobDefinition.builder()
                .jobName("retryJob").cron("0/10 * * * * ?")
                .shardingTotalCount(1).misfirePolicy(MisfirePolicy.IGNORE).build(), flaky);
        scheduler.workerRegistry().register("w1", clock.nanoTime());
        scheduler.tick();
        clock.advanceMillis(10_000L);
        scheduler.tick();

        assertEquals(3, calls.get());
        assertEquals(ExecutionState.SUCCESS,
                scheduler.ledger().get(ExecutionRecord.key("retryJob", T0 + 10_000L, 0)).state());
        assertEquals(2L, scheduler.metrics().failed());
        assertEquals(2L, scheduler.metrics().retried());
        assertEquals(3L, scheduler.metrics().shards());
    }

    @Test
    void failedExecutionIsSuppressedOnceAttemptsAreExhausted() {
        MutableClock clock = new MutableClock(T0);
        AtomicInteger calls = new AtomicInteger();
        Scheduler scheduler = Scheduler.builder()
                .clock(clock).zone(ZoneOffset.UTC)
                .workerTimeoutNanos(Long.MAX_VALUE).maxAttempts(2).build();
        scheduler.registerJob(JobDefinition.builder()
                .jobName("alwaysFails").cron("0/10 * * * * ?")
                .shardingTotalCount(1).misfirePolicy(MisfirePolicy.IGNORE).build(),
                ctx -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("permanent failure");
                });
        scheduler.workerRegistry().register("w1", clock.nanoTime());
        scheduler.tick();
        clock.advanceMillis(10_000L);
        scheduler.tick();

        assertEquals(2, calls.get());
        assertEquals(ExecutionState.FAILED,
                scheduler.ledger().get(ExecutionRecord.key("alwaysFails", T0 + 10_000L, 0)).state());
        assertEquals(2L, scheduler.metrics().failed());

        scheduler.lastFireStore().update("alwaysFails", T0);
        scheduler.tick();
        assertEquals(2, calls.get(), "no further attempts after maxAttempts");
        assertTrue(scheduler.metrics().duplicatesSuppressed() >= 1L);
    }
}
