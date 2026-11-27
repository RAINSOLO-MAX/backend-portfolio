package dev.rainsolo.scheduler.scheduler;

import dev.rainsolo.scheduler.job.JobDefinition;
import dev.rainsolo.scheduler.job.JobListener;
import dev.rainsolo.scheduler.job.ShardContext;
import dev.rainsolo.scheduler.sharding.ShardingStrategy;
import dev.rainsolo.scheduler.worker.WorkerRegistry;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core coordinator. {@link #tick()} is a deterministic, manually driven step: detect due
 * fire times, compensate misfires, assign shards to alive workers, then execute every shard
 * through the idempotent ledger. {@link #start()} / {@link #stop()} add an optional driver
 * thread that calls the very same tick method.
 */
public final class SchedulerCoordinator {

    /** Job definition paired with its listener. */
    private record Registration(JobDefinition definition, JobListener listener) {
    }

    private final Clock clock;
    private final ZoneId zone;
    private final LastFireStore lastFireStore;
    private final ExecutionLedger ledger;
    private final ShardingStrategy shardingStrategy;
    private final WorkerRegistry workerRegistry;
    private final long workerTimeoutNanos;
    private final MissedFireDetector misfireDetector;
    private final FailoverRebalancer failoverRebalancer;
    private final SchedulerMetrics metrics;
    private final long driverIntervalMillis;
    private final Map<String, Registration> jobs = new ConcurrentHashMap<>();

    private volatile boolean running;
    private Thread driver;

    public SchedulerCoordinator(Clock clock, ZoneId zone, LastFireStore lastFireStore,
                                ExecutionLedger ledger, ShardingStrategy shardingStrategy,
                                WorkerRegistry workerRegistry, long workerTimeoutNanos,
                                long driverIntervalMillis) {
        this.clock = clock;
        this.zone = zone;
        this.lastFireStore = lastFireStore;
        this.ledger = ledger;
        this.shardingStrategy = shardingStrategy;
        this.workerRegistry = workerRegistry;
        this.workerTimeoutNanos = workerTimeoutNanos;
        this.misfireDetector = new MissedFireDetector(zone);
        this.failoverRebalancer = new FailoverRebalancer();
        this.metrics = new SchedulerMetrics();
        this.driverIntervalMillis = driverIntervalMillis;
    }

    /** Registers (or replaces) a job and its listener. */
    public void registerJob(JobDefinition definition, JobListener listener) {
        jobs.put(definition.jobName(), new Registration(definition, listener));
    }

    /** Runs exactly one deterministic scheduling step using the injected clock. */
    public void tick() {
        long now = clock.wallTimeMillis();
        List<String> alive = workerRegistry.aliveWorkers(clock.nanoTime(), workerTimeoutNanos);
        for (Map.Entry<String, Registration> entry : jobs.entrySet()) {
            String jobName = entry.getKey();
            Registration registration = entry.getValue();
            JobDefinition job = registration.definition();
            Long stored = lastFireStore.lastFire(jobName);
            long base = stored != null ? stored : now;
            if (stored == null) {
                lastFireStore.update(jobName, base);
            }
            MissedFireDetector.Result detection = misfireDetector.detect(job.cron(), job.misfirePolicy(), base, now);
            if (detection.updatedLastFireMillis() > base) {
                lastFireStore.update(jobName, detection.updatedLastFireMillis());
            }
            if (detection.misfired() > 0) {
                metrics.incMisfired(detection.misfired());
            }
            for (long fireTime : detection.fireTimes()) {
                metrics.incTriggered();
                FailoverRebalancer.Result rebalanced = failoverRebalancer.rebalance(
                        jobName, alive, job.shardingTotalCount(), shardingStrategy);
                if (job.failoverEnabled()) {
                    metrics.incFailover(rebalanced.reassignedShards().size());
                }
                for (Map.Entry<String, List<Integer>> assignment : rebalanced.assignment().entrySet()) {
                    for (int shardItem : assignment.getValue()) {
                        ShardContext context = new ShardContext(jobName, fireTime, shardItem,
                                job.shardParameter(shardItem));
                        executeShard(job, registration.listener(), context, fireTime);
                    }
                }
            }
        }
    }

    private void executeShard(JobDefinition job, JobListener listener, ShardContext context, long fireTime) {
        for (int attempt = 0; attempt < ledger.maxAttempts(); attempt++) {
            ExecutionRecord candidate = new ExecutionRecord(job.jobName(), fireTime, context.shardItem(), context);
            ExecutionRecord claimed = ledger.tryClaim(candidate);
            if (claimed == null) {
                metrics.incDuplicatesSuppressed();
                return;
            }
            if (attempt > 0) {
                metrics.incRetried();
            }
            metrics.incShards();
            long start = clock.wallTimeMillis();
            try {
                listener.execute(context);
                long elapsed = clock.wallTimeMillis() - start;
                if (job.timeoutMillis() > 0 && elapsed > job.timeoutMillis()) {
                    ledger.markTimeout(claimed);
                    metrics.incFailed();
                    continue;
                }
                ledger.markSuccess(claimed);
                return;
            } catch (Exception e) {
                ledger.markFailed(claimed);
                metrics.incFailed();
            }
        }
    }

    /** Starts a daemon driver thread that repeatedly invokes {@link #tick()}. */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        driver = new Thread(this::driverLoop, "scheduler-driver");
        driver.setDaemon(true);
        driver.start();
    }

    private void driverLoop() {
        while (running) {
            try {
                tick();
                Thread.sleep(driverIntervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Stops the driver thread and waits for it to exit; manual {@link #tick()} calls remain available. */
    public synchronized void stop() {
        running = false;
        Thread current = driver;
        if (current != null) {
            current.interrupt();
            try {
                current.join(1_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            driver = null;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public SchedulerMetrics metrics() {
        return metrics;
    }

    public ExecutionLedger ledger() {
        return ledger;
    }

    public WorkerRegistry workerRegistry() {
        return workerRegistry;
    }

    public LastFireStore lastFireStore() {
        return lastFireStore;
    }

    public ZoneId zone() {
        return zone;
    }

    public Clock clock() {
        return clock;
    }
}
