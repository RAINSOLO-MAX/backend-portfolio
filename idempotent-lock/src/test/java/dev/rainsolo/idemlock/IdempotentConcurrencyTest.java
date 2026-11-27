package dev.rainsolo.idemlock;

import dev.rainsolo.idemlock.core.ExecutionResult;
import dev.rainsolo.idemlock.core.IdempotentConfig;
import dev.rainsolo.idemlock.core.Outcome;
import dev.rainsolo.idemlock.executor.IdempotentExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotentConcurrencyTest {

    @Test
    void thirtyTwoThreadsExecuteActionExactlyOnce() throws Exception {
        int threads = 32;
        String key = "pay-callback-42";
        IdempotentConfig config = IdempotentConfig.builder()
                .ttlMillis(5_000)
                .waitForResult(true)
                .waitTimeoutMillis(3_000)
                .build();
        IdempotentExecutor executor = new IdempotentExecutor(config);
        AtomicInteger actionCalls = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<ExecutionResult<String>>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return executor.execute(key, () -> {
                    actionCalls.incrementAndGet();
                    try {
                        Thread.sleep(80);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "ORDER-42-PAID";
                });
            }));
        }
        start.countDown();
        List<ExecutionResult<String>> results = new ArrayList<>();
        for (Future<ExecutionResult<String>> future : futures) {
            results.add(future.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, actionCalls.get(), "action must run exactly once under concurrency");
        int executed = 0;
        int reused = 0;
        for (ExecutionResult<String> result : results) {
            assertEquals("ORDER-42-PAID", result.value());
            assertTrue(result.outcome() == Outcome.EXECUTED || result.outcome() == Outcome.REUSED);
            if (result.outcome() == Outcome.EXECUTED) {
                executed++;
            } else {
                reused++;
            }
        }
        assertEquals(1, executed);
        assertEquals(threads - 1, reused);
        assertEquals(1, executor.metrics().snapshot().executed());
        assertEquals(threads - 1, executor.metrics().snapshot().reused());
    }
}
