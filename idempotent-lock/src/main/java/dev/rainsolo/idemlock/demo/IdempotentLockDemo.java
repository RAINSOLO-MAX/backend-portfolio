package dev.rainsolo.idemlock.demo;

import dev.rainsolo.idemlock.core.ExecutionResult;
import dev.rainsolo.idemlock.core.IdempotentConfig;
import dev.rainsolo.idemlock.executor.IdempotentExecutor;
import dev.rainsolo.idemlock.lock.DistributedLock;
import dev.rainsolo.idemlock.lock.FencingTokenGuard;
import dev.rainsolo.idemlock.lock.LockManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** End-to-end demo: duplicate payment callbacks and a GC-pause stale-write scenario. */
public final class IdempotentLockDemo {

    private IdempotentLockDemo() {
    }

    public static void main(String[] args) throws Exception {
        IdempotentExecutor executor = new IdempotentExecutor(
                IdempotentConfig.builder().ttlMillis(30_000).build());
        scenarioA(executor);
        LockManager manager = LockManager.builder()
                .defaultLeaseMillis(100).watchdog(false).build();
        FencingTokenGuard guard = new FencingTokenGuard();
        scenarioB(manager, guard);

        System.out.println("=== METRICS ===");
        System.out.println("IDEMPOTENT " + executor.metrics().snapshot());
        System.out.println("LOCK " + manager.metrics().snapshot());
        System.out.println("GUARD lastAccepted=" + guard.lastAccepted()
                + " rejected=" + guard.rejectedCount());
    }

    private static void scenarioA(IdempotentExecutor executor) throws Exception {
        System.out.println("=== SCENARIO A: duplicate payment callbacks ===");
        AtomicInteger debitCount = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(5);
        List<Future<String>> futures = new ArrayList<>();

        for (int i = 1; i <= 5; i++) {
            final int sequence = i;
            futures.add(pool.submit(() -> {
                start.await();
                ExecutionResult<String> result = executor.execute("pay:order-1001", () -> {
                    debitCount.incrementAndGet();
                    try {
                        Thread.sleep(60);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "CHARGED-1001";
                });
                return "CALLBACK-" + sequence + " outcome=" + result.outcome()
                        + " value=" + result.value();
            }));
        }
        start.countDown();
        for (Future<String> future : futures) {
            System.out.println(future.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        System.out.println("DEBIT_COUNT=" + debitCount.get());
    }

    private static void scenarioB(LockManager manager, FencingTokenGuard guard) throws Exception {
        System.out.println("=== SCENARIO B: GC pause and fencing token ===");
        CountDownLatch firstLocked = new CountDownLatch(1);
        AtomicLong firstToken = new AtomicLong();
        AtomicLong secondToken = new AtomicLong();

        Thread first = new Thread(() -> {
            DistributedLock lock = manager.lock("account-writer");
            if (!lock.tryLock(100)) {
                System.out.println("T1 LOCK FAILED");
                return;
            }
            long token = lock.fencingToken();
            firstToken.set(token);
            System.out.println("T1 ACQUIRED token=" + token);
            firstLocked.countDown();
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            boolean accepted = guard.accept(token);
            if (accepted) {
                System.out.println("T1 WRITE ACCEPTED token=" + token);
            } else {
                System.out.println("REJECTED stale token=" + token);
            }
            lock.unlock();
        }, "writer-1");

        first.start();
        firstLocked.await(2, TimeUnit.SECONDS);
        Thread.sleep(180);

        Thread second = new Thread(() -> {
            DistributedLock lock = manager.lock("account-writer");
            if (!lock.tryLock(100)) {
                System.out.println("T2 LOCK FAILED");
                return;
            }
            long token = lock.fencingToken();
            secondToken.set(token);
            boolean accepted = guard.accept(token);
            System.out.println("T2 ACQUIRED token=" + token + " writeAccepted=" + accepted);
            lock.unlock();
        }, "writer-2");

        second.start();
        first.join();
        second.join();
        System.out.println("TOKENS first=" + firstToken.get() + " second=" + secondToken.get()
                + " strictlyIncreasing=" + (secondToken.get() > firstToken.get()));
    }
}
