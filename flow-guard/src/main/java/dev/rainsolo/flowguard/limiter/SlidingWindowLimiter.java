package dev.rainsolo.flowguard.limiter;

import dev.rainsolo.flowguard.core.Clock;
import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.PermitResult;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Divides the window into a ring of small slots. Each request is counted in its slot;
 * slots that fall out of the trailing window are cleared, so traffic is smooth across
 * a boundary instead of resetting all at once.
 */
public final class SlidingWindowLimiter extends AbstractRateLimiter {

    private final int slots;
    private final long slotMillis;
    private final long windowPermits;
    private final AtomicLong[] counters;
    private final long[] slotIds;

    public SlidingWindowLimiter(LimiterConfig config, Clock clock) {
        super(config, clock);
        this.slots = config.slidingSlots();
        this.slotMillis = Math.max(1L, config.windowMillis() / slots);
        this.windowPermits = config.windowPermits();
        this.counters = new AtomicLong[slots];
        this.slotIds = new long[slots];
        for (int i = 0; i < slots; i++) {
            counters[i] = new AtomicLong();
            slotIds[i] = Long.MIN_VALUE;
        }
    }

    @Override
    public synchronized PermitResult tryAcquire(String key, int permits) {
        checkPermits(permits);
        long nowMillis = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime());
        long slotId = nowMillis / slotMillis;
        long windowStartSlot = slotId - slots + 1;
        int index = Math.floorMod(slotId, slots);

        long total = 0;
        long oldestActiveSlot = Long.MAX_VALUE;
        for (int i = 0; i < slots; i++) {
            long id = slotIds[i];
            if (id < windowStartSlot || id > slotId) {
                counters[i].set(0);
                slotIds[i] = Long.MIN_VALUE;
            } else {
                long count = counters[i].get();
                total += count;
                if (count > 0 && id < oldestActiveSlot) {
                    oldestActiveSlot = id;
                }
            }
        }
        // Reuse the ring slot for the current time slice; it was cleared above.
        if (slotIds[index] != slotId) {
            counters[index].set(0);
            slotIds[index] = slotId;
        }

        if (total + permits <= windowPermits) {
            counters[index].addAndGet(permits);
            recordAllowed(permits);
            return PermitResult.permit(windowPermits - total - permits);
        }
        recordRejected(permits);
        long waitMillis = oldestActiveSlot == Long.MAX_VALUE
                ? slotMillis
                : Math.max(1L, (oldestActiveSlot + slots) * slotMillis - nowMillis);
        return PermitResult.reject(waitMillis);
    }

    @Override
    protected synchronized long availablePermits() {
        long total = 0;
        for (AtomicLong counter : counters) {
            total += counter.get();
        }
        return Math.max(0L, windowPermits - total);
    }
}
