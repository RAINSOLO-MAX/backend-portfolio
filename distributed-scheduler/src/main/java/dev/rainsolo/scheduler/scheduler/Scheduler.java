package dev.rainsolo.scheduler.scheduler;

import dev.rainsolo.scheduler.job.JobDefinition;
import dev.rainsolo.scheduler.job.JobListener;
import dev.rainsolo.scheduler.sharding.AverageShardingStrategy;
import dev.rainsolo.scheduler.sharding.ShardingStrategy;
import dev.rainsolo.scheduler.worker.WorkerRegistry;

import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/** Fluent facade over {@link SchedulerCoordinator}; all configuration goes through the builder. */
public final class Scheduler {

    private final SchedulerCoordinator coordinator;

    private Scheduler(SchedulerCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    /** Returns a new builder with deterministic defaults (UTC, in-memory store, average sharding). */
    public static Builder builder() {
        return new Builder();
    }

    public void registerJob(JobDefinition definition, JobListener listener) {
        coordinator.registerJob(definition, listener);
    }

    public void tick() {
        coordinator.tick();
    }

    public void start() {
        coordinator.start();
    }

    public void stop() {
        coordinator.stop();
    }

    public SchedulerMetrics.Snapshot metrics() {
        return coordinator.metrics().snapshot();
    }

    public WorkerRegistry workerRegistry() {
        return coordinator.workerRegistry();
    }

    public LastFireStore lastFireStore() {
        return coordinator.lastFireStore();
    }

    public ExecutionLedger ledger() {
        return coordinator.ledger();
    }

    public SchedulerCoordinator coordinator() {
        return coordinator;
    }

    /** Builder for a {@link Scheduler}. */
    public static final class Builder {
        private Clock clock = new SystemClock();
        private ZoneId zone = ZoneOffset.UTC;
        private Path lastFirePath;
        private ShardingStrategy shardingStrategy = new AverageShardingStrategy();
        private WorkerRegistry workerRegistry = new WorkerRegistry();
        private long workerTimeoutNanos = TimeUnit.SECONDS.toNanos(30);
        private int maxAttempts = 3;
        private long driverIntervalMillis = 100L;

        public Builder clock(Clock value) {
            this.clock = value;
            return this;
        }

        public Builder zone(ZoneId value) {
            this.zone = value;
            return this;
        }

        /** When set, last-fire markers are persisted to this file and reloaded on rebuild. */
        public Builder lastFirePath(Path value) {
            this.lastFirePath = value;
            return this;
        }

        public Builder shardingStrategy(ShardingStrategy value) {
            this.shardingStrategy = value;
            return this;
        }

        public Builder workerRegistry(WorkerRegistry value) {
            this.workerRegistry = value;
            return this;
        }

        public Builder workerTimeoutNanos(long value) {
            this.workerTimeoutNanos = value;
            return this;
        }

        public Builder maxAttempts(int value) {
            this.maxAttempts = value;
            return this;
        }

        public Builder driverIntervalMillis(long value) {
            this.driverIntervalMillis = value;
            return this;
        }

        public Scheduler build() {
            LastFireStore store = lastFirePath == null
                    ? LastFireStore.inMemory()
                    : new LastFireStore(lastFirePath);
            SchedulerCoordinator coordinator = new SchedulerCoordinator(clock, zone, store,
                    new ExecutionLedger(maxAttempts), shardingStrategy, workerRegistry,
                    workerTimeoutNanos, driverIntervalMillis);
            return new Scheduler(coordinator);
        }
    }
}
