package dev.rainsolo.delayqueue;

import dev.rainsolo.delayqueue.config.QueueConfig;
import dev.rainsolo.delayqueue.core.Clock;
import dev.rainsolo.delayqueue.core.DelayedMessage;
import dev.rainsolo.delayqueue.core.MessageStatus;
import dev.rainsolo.delayqueue.core.SystemClock;
import dev.rainsolo.delayqueue.delivery.DeadLetterQueue;
import dev.rainsolo.delayqueue.delivery.DeliveryListener;
import dev.rainsolo.delayqueue.delivery.RetryStrategy;
import dev.rainsolo.delayqueue.idempotent.IdempotentStore;
import dev.rainsolo.delayqueue.idempotent.InMemoryIdempotentStore;
import dev.rainsolo.delayqueue.metrics.QueueMetrics;
import dev.rainsolo.delayqueue.store.InMemoryMessageStore;
import dev.rainsolo.delayqueue.store.MessageStore;
import dev.rainsolo.delayqueue.store.SnapshotStore;
import dev.rainsolo.delayqueue.wheel.HierarchicalTimeWheel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Facade wiring the timing wheel, at-least-once delivery, idempotency, DLQ and snapshots. */
public final class DelayQueueService implements AutoCloseable {

    private final Clock clock;
    private final QueueConfig config;
    private final HierarchicalTimeWheel wheel;
    private final MessageStore store = new InMemoryMessageStore();
    private final IdempotentStore idempotent = new InMemoryIdempotentStore();
    private final DeadLetterQueue deadLetters = new DeadLetterQueue();
    private final SnapshotStore snapshots;
    private final QueueMetrics metrics = new QueueMetrics();
    private final Map<String, DeliveryListener> listeners = new ConcurrentHashMap<>();

    public DelayQueueService(QueueConfig config, Clock clock, Path snapshotFile, boolean autoStart) {
        this.clock = clock;
        this.config = config;
        this.snapshots = new SnapshotStore(snapshotFile);
        this.wheel = new HierarchicalTimeWheel(clock, config, this::dispatch);
        if (autoStart) {
            wheel.start();
        }
    }

    public static DelayQueueService create(Path snapshotFile) {
        return new DelayQueueService(QueueConfig.defaults(), new SystemClock(), snapshotFile, true);
    }

    /** No background thread: the caller drives progress with {@link #tick()}. Useful for deterministic tests. */
    public static DelayQueueService manual(QueueConfig config, Clock clock, Path snapshotFile) {
        return new DelayQueueService(config, clock, snapshotFile, false);
    }

    public void registerListener(String topic, DeliveryListener listener) {
        listeners.put(topic, listener);
    }

    public String submit(String id, String topic, String payload, long delayMillis) {
        return submit(id, topic, payload, delayMillis, config.defaultMaxAttempts());
    }

    public String submit(String id, String topic, String payload, long delayMillis, int maxAttempts) {
        long deadline = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, delayMillis));
        DelayedMessage message = new DelayedMessage(id, topic, payload, deadline,
                maxAttempts, clock.wallTimeMillis());
        store.save(message);
        wheel.submit(message);
        metrics.incSubmitted();
        return id;
    }

    private void dispatch(DelayedMessage message) {
        metrics.incExpired();
        if (idempotent.isProcessed(message.getId())) {
            metrics.incDuplicateSuppressed();
            return;
        }
        message.setStatus(MessageStatus.DELIVERING);
        store.save(message);
        DeliveryListener listener = listeners.get(message.getTopic());
        try {
            if (listener != null) {
                listener.onMessage(message);
            }
            idempotent.markProcessed(message.getId());
            message.setStatus(MessageStatus.SUCCEEDED);
            store.save(message);
            metrics.incSucceeded();
        } catch (Exception failure) {
            int attempt = message.incrementAttempts();
            if (attempt >= message.getMaxAttempts()) {
                message.setStatus(MessageStatus.DEAD);
                store.save(message);
                deadLetters.add(message);
                metrics.incDeadLettered();
            } else {
                long backoff = RetryStrategy.backoffMillis(attempt, config.baseBackoffMillis(),
                        config.backoffFactor(), config.maxBackoffMillis());
                message.setDeadlineNanos(clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(backoff));
                message.setStatus(MessageStatus.PENDING);
                store.save(message);
                wheel.submit(message);
                metrics.incRetried();
            }
        }
    }

    /** Drive the wheel manually (also used by the internal background driver). */
    public void tick() {
        wheel.tick();
    }

    public void snapshot() throws IOException {
        snapshots.write(store.all(), clock);
    }

    /**
     * Re-schedules every unfinished message from the snapshot. A message that was
     * DELIVERING at crash time is deliberately re-delivered (at-least-once semantics).
     *
     * @return number of restored messages
     */
    public int restore() throws IOException {
        int restored = 0;
        for (SnapshotStore.Entry entry : snapshots.read()) {
            DelayedMessage existing = store.find(entry.id());
            if (existing != null && (existing.getStatus() == MessageStatus.SUCCEEDED
                    || existing.getStatus() == MessageStatus.DEAD)) {
                continue;
            }
            long deadline = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(entry.remainingMillis());
            DelayedMessage message = new DelayedMessage(entry.id(), entry.topic(), entry.payload(),
                    deadline, entry.maxAttempts(), entry.createdWallMillis());
            for (int i = 0; i < entry.attempts(); i++) {
                message.incrementAttempts();
            }
            store.save(message);
            wheel.submit(message);
            metrics.incSubmitted();
            restored++;
        }
        return restored;
    }

    public QueueMetrics.Snapshot metrics() {
        return metrics.snapshot();
    }

    public DeadLetterQueue deadLetters() {
        return deadLetters;
    }

    public int pendingCount() {
        return wheel.pendingCount();
    }

    @Override
    public void close() {
        wheel.close();
    }
}
