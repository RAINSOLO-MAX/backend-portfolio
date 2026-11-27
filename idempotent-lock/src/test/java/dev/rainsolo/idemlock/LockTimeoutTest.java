package dev.rainsolo.idemlock;

import dev.rainsolo.idemlock.lock.DistributedLock;
import dev.rainsolo.idemlock.lock.LockManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockTimeoutTest {

    @Test
    void leaseWithoutRenewalExpiresAndCanBeAcquiredByAnotherThread() throws Exception {
        LockManager manager = LockManager.builder().defaultLeaseMillis(150).watchdog(false).build();
        DistributedLock first = manager.lock("timeout");
        assertTrue(first.tryLock(150));
        long firstToken = first.fencingToken();
        assertTrue(firstToken > 0);

        Thread.sleep(300);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> acquired = pool.submit(() -> {
                DistributedLock second = manager.lock("timeout");
                if (!second.tryLock(150)) {
                    return false;
                }
                boolean valid = second.isHeldByCurrentThread()
                        && second.fencingToken() > firstToken;
                second.unlock();
                return valid;
            });
            assertTrue(acquired.get(3, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertFalse(first.isHeldByCurrentThread());
        assertEquals(-1L, first.fencingToken());
        first.unlock();
        assertTrue(manager.metrics().snapshot().expired() >= 1);
        assertEquals(2, manager.metrics().snapshot().acquired());
    }
}
