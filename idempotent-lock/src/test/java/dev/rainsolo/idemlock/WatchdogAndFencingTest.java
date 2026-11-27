package dev.rainsolo.idemlock;

import dev.rainsolo.idemlock.lock.DistributedLock;
import dev.rainsolo.idemlock.lock.FencingTokenGuard;
import dev.rainsolo.idemlock.lock.LockManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatchdogAndFencingTest {

    @Test
    void watchdogKeepsLockAcrossLongTask() throws Exception {
        LockManager manager = LockManager.builder().defaultLeaseMillis(100).watchdog(true).build();
        DistributedLock first = manager.lock("watchdog");
        assertTrue(first.tryLock(100));
        long firstToken = first.fencingToken();

        Thread.sleep(350);

        DistributedLock intruder = manager.lock("watchdog");
        assertFalse(intruder.tryLock(100), "watchdog must keep the lease alive");
        assertTrue(first.isHeldByCurrentThread());
        assertTrue(manager.metrics().snapshot().renewed() >= 2);

        first.unlock();
        assertTrue(intruder.tryLock(100));
        assertTrue(intruder.fencingToken() > firstToken);
        intruder.unlock();
    }

    @Test
    void expiredLeaseYieldsLargerTokenAndStaleWriteIsRejected() throws Exception {
        LockManager manager = LockManager.builder().defaultLeaseMillis(100).watchdog(false).build();
        DistributedLock first = manager.lock("fenced-resource");
        assertTrue(first.tryLock(100));
        long staleToken = first.fencingToken();

        Thread.sleep(220);

        DistributedLock second = manager.lock("fenced-resource");
        assertTrue(second.tryLock(100), "expired lease must be acquirable");
        long freshToken = second.fencingToken();
        assertTrue(freshToken > staleToken);

        FencingTokenGuard guard = new FencingTokenGuard();
        assertTrue(guard.accept(freshToken));
        assertFalse(guard.accept(staleToken), "stale token from GC-paused holder must be rejected");
        assertEquals(1, guard.rejectedCount());
        assertEquals(freshToken, guard.lastAccepted());

        assertFalse(first.isHeldByCurrentThread());
        second.unlock();
        first.unlock();
        assertTrue(manager.metrics().snapshot().expired() >= 1);
    }

    @Test
    void wallClockRewindDoesNotAffectLease() {
        MutableClock clock = new MutableClock();
        LockManager manager = LockManager.builder()
                .clock(clock).defaultLeaseMillis(1_000).watchdog(false).build();
        DistributedLock first = manager.lock("rewind");
        assertTrue(first.tryLock(1_000));
        long firstToken = first.fencingToken();

        clock.rewindWallClock(86_400_000L);
        clock.setWallTimeMillis(0L);
        assertTrue(first.isHeldByCurrentThread(), "wall-clock rewind must not expire a nanoTime lease");
        assertEquals(firstToken, first.fencingToken());

        clock.advanceNanos(TimeUnit.MILLISECONDS.toNanos(1_001));
        assertFalse(first.isHeldByCurrentThread());
        DistributedLock second = manager.lock("rewind");
        assertTrue(second.tryLock(1_000));
        assertTrue(second.fencingToken() > firstToken);
        second.unlock();
        first.unlock();
    }
}
