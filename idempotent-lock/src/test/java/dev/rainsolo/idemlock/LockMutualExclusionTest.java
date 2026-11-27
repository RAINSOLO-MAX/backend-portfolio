package dev.rainsolo.idemlock;

import dev.rainsolo.idemlock.lock.DistributedLock;
import dev.rainsolo.idemlock.lock.LockManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockMutualExclusionTest {

    @Test
    void eightThreadsNeverOverlapInCriticalSection() throws Exception {
        int threads = 8;
        LockManager manager = LockManager.builder().defaultLeaseMillis(2_000).watchdog(true).build();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                DistributedLock lock = manager.lock("critical-section");
                if (!lock.tryLock(2_000, 5_000)) {
                    return false;
                }
                try {
                    maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                    Thread.sleep(10);
                    counter.incrementAndGet();
                } finally {
                    active.decrementAndGet();
                    lock.unlock();
                }
                return true;
            }));
        }
        start.countDown();
        for (Future<Boolean> future : futures) {
            assertTrue(future.get(10, TimeUnit.SECONDS), "every thread must acquire the lock");
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, maxActive.get(), "critical section must never overlap");
        assertEquals(threads, counter.get());
        assertEquals(threads, manager.metrics().snapshot().acquired());
    }

    @Test
    void reentrantAcquireTracksHoldCount() {
        LockManager manager = LockManager.builder().defaultLeaseMillis(1_000).watchdog(false).build();
        DistributedLock lock = manager.lock("reentrant");

        assertTrue(lock.tryLock(1_000));
        assertEquals(1, lock.holdCount());
        assertTrue(lock.tryLock(1_000));
        assertEquals(2, lock.holdCount());
        assertTrue(lock.isHeldByCurrentThread());
        assertTrue(lock.fencingToken() > 0);

        lock.unlock();
        assertEquals(1, lock.holdCount());
        assertTrue(lock.isHeldByCurrentThread());
        lock.unlock();
        assertEquals(0, lock.holdCount());
        assertFalse(lock.isHeldByCurrentThread());
        assertEquals(-1L, lock.fencingToken());
        assertEquals(1, manager.metrics().snapshot().acquired());
        assertEquals(1, manager.metrics().snapshot().released());
    }

    @Test
    void unlockByNonOwnerThrowsAndKeepsLease() throws Exception {
        LockManager manager = LockManager.builder().defaultLeaseMillis(1_000).watchdog(false).build();
        DistributedLock lock = manager.lock("guarded");
        assertTrue(lock.tryLock(1_000));
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Thread intruder = new Thread(() -> {
            try {
                lock.unlock();
            } catch (Throwable error) {
                caught.set(error);
            }
        }, "non-owner");
        intruder.start();
        intruder.join();

        assertInstanceOf(IllegalMonitorStateException.class, caught.get());
        assertTrue(lock.isHeldByCurrentThread());
        lock.unlock();
        assertFalse(lock.isHeldByCurrentThread());
    }
}
