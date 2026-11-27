package dev.rainsolo.delayqueue.wheel;

import dev.rainsolo.delayqueue.core.DelayedMessage;

import java.util.ArrayList;
import java.util.List;

/** One layer of the timing wheel. All methods are called under the owning wheel's lock. */
final class WheelLevel {

    private final long tickNanos;
    private final int size;
    private final List<List<DelayedMessage>> buckets = new ArrayList<>();
    private long currentTick;

    WheelLevel(long tickMillis, int size) {
        this.tickNanos = tickMillis * 1_000_000L;
        this.size = size;
        for (int i = 0; i < size; i++) {
            buckets.add(new ArrayList<>());
        }
    }

    long maxDurationNanos() {
        return tickNanos * size;
    }

    /** @return the bucket index this message was placed in */
    int place(DelayedMessage message, long delayNanos) {
        long ticksFromNow = Math.max(1, delayNanos / tickNanos);
        int index = (int) ((currentTick + ticksFromNow) % size);
        buckets.get(index).add(message);
        return index;
    }

    /** Advances one tick and returns everything scheduled in the now-current bucket. */
    List<DelayedMessage> advanceAndTake() {
        currentTick++;
        int index = (int) (currentTick % size);
        List<DelayedMessage> due = buckets.get(index);
        buckets.set(index, new ArrayList<>());
        return due;
    }

    int size() {
        int total = 0;
        for (List<DelayedMessage> bucket : buckets) {
            total += bucket.size();
        }
        return total;
    }
}
