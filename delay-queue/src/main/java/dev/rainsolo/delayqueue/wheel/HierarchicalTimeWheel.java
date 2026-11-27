package dev.rainsolo.delayqueue.wheel;

import dev.rainsolo.delayqueue.config.QueueConfig;
import dev.rainsolo.delayqueue.core.Clock;
import dev.rainsolo.delayqueue.core.DelayedMessage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Three-level timing wheel (millis / centis / seconds) plus an overflow heap.
 * Tasks in higher levels are pulled down one level at a time until the lowest wheel
 * expires them, which keeps memory bounded while supporting hour-long delays.
 *
 * All deadlines are monotonic nanoseconds; the wall clock is never used for scheduling.
 */
public final class HierarchicalTimeWheel implements AutoCloseable {

    private static final long MAX_CATCHUP_TICKS = 200_000;

    private final Clock clock;
    private final long tickNanos;
    private final int l1Ratio;
    private final int l2Ratio;
    private final WheelLevel l0;
    private final WheelLevel l1;
    private final WheelLevel l2;
    private final PriorityQueue<DelayedMessage> overflow =
            new PriorityQueue<>(Comparator.comparingLong(DelayedMessage::getDeadlineNanos));
    private final Consumer<DelayedMessage> onExpire;

    private long lastTickNanos;
    private long l0Ticks;
    private long l1Ticks;
    private volatile boolean running;
    private Thread driver;

    public HierarchicalTimeWheel(Clock clock, QueueConfig config, Consumer<DelayedMessage> onExpire) {
        this.clock = clock;
        this.tickNanos = config.tickMillis() * 1_000_000L;
        this.l1Ratio = config.l1Ratio();
        this.l2Ratio = config.l2Ratio();
        this.l0 = new WheelLevel(config.tickMillis(), config.l0Slots());
        this.l1 = new WheelLevel(config.l1TickMillis(), config.l1Slots());
        this.l2 = new WheelLevel(config.l2TickMillis(), config.l2Slots());
        this.onExpire = onExpire;
        this.lastTickNanos = clock.nanoTime();
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        lastTickNanos = clock.nanoTime();
        driver = new Thread(this::drive, "delay-queue-wheel");
        driver.setDaemon(true);
        driver.start();
    }

    private void drive() {
        while (running) {
            tick();
            LockSupport.parkNanos(tickNanos);
        }
    }

    /** Advance the wheel to the current monotonic time. Public for deterministic tests. */
    public void tick() {
        List<DelayedMessage> due = new ArrayList<>();
        synchronized (this) {
            advance(clock.nanoTime(), due);
        }
        for (DelayedMessage message : due) {
            onExpire.accept(message);
        }
    }

    public void submit(DelayedMessage message) {
        synchronized (this) {
            place(message, clock.nanoTime(), null);
        }
    }

    public synchronized int pendingCount() {
        return l0.size() + l1.size() + l2.size() + overflow.size();
    }

    private void advance(long now, List<DelayedMessage> due) {
        long catchUp = 0;
        while (lastTickNanos + tickNanos <= now && catchUp < MAX_CATCHUP_TICKS) {
            lastTickNanos += tickNanos;
            l0Ticks++;
            catchUp++;
            reschedule(l0.advanceAndTake(), due);
            if (l0Ticks % l1Ratio == 0) {
                l1Ticks++;
                reschedule(l1.advanceAndTake(), due);
                if (l1Ticks % l2Ratio == 0) {
                    reschedule(l2.advanceAndTake(), due);
                }
            }
            drainOverflow(due);
        }
        if (catchUp >= MAX_CATCHUP_TICKS) {
            lastTickNanos = now;
        }
    }

    private void drainOverflow(List<DelayedMessage> due) {
        long horizon = lastTickNanos + l2.maxDurationNanos();
        while (!overflow.isEmpty() && overflow.peek().getDeadlineNanos() <= horizon) {
            DelayedMessage candidate = overflow.poll();
            if (candidate.getDeadlineNanos() - lastTickNanos >= l2.maxDurationNanos()) {
                // Still too far for every wheel level: leave it in the heap and retry next tick.
                // Without this guard, polling and re-adding the same entry would loop forever.
                overflow.add(candidate);
                break;
            }
            place(candidate, lastTickNanos, due);
        }
    }

    private void reschedule(List<DelayedMessage> messages, List<DelayedMessage> due) {
        for (DelayedMessage message : messages) {
            place(message, lastTickNanos, due);
        }
    }

    /** @param due when null, this is an external submit and immediate expiry is scheduled on the wheel */
    private void place(DelayedMessage message, long now, List<DelayedMessage> due) {
        long delay = message.getDeadlineNanos() - now;
        if (delay <= 0) {
            if (due != null) {
                due.add(message);
            } else {
                l0.place(message, tickNanos);
            }
            return;
        }
        if (delay < l0.maxDurationNanos()) {
            l0.place(message, delay);
        } else if (delay < l1.maxDurationNanos()) {
            l1.place(message, delay);
        } else if (delay < l2.maxDurationNanos()) {
            l2.place(message, delay);
        } else {
            overflow.add(message);
        }
    }

    @Override
    public void close() {
        running = false;
        if (driver != null) {
            driver.interrupt();
        }
    }
}
