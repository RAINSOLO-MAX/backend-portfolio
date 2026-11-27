package dev.rainsolo.idemlock;

import dev.rainsolo.idemlock.core.ExecutionResult;
import dev.rainsolo.idemlock.core.IdempotentConfig;
import dev.rainsolo.idemlock.core.Outcome;
import dev.rainsolo.idemlock.executor.IdempotentExecutor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotentStateMachineTest {

    @Test
    void failedRecordCanBeRetriedAndThenCached() {
        IdempotentConfig config = IdempotentConfig.builder().ttlMillis(10_000).maxAttempts(3).build();
        IdempotentExecutor executor = new IdempotentExecutor(config);
        AtomicInteger attempts = new AtomicInteger();
        Supplier<String> flaky = () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("downstream timeout");
            }
            return "OK";
        };

        ExecutionResult<String> first = executor.execute("retry-key", flaky);
        assertEquals(Outcome.FAILED, first.outcome());
        assertEquals("downstream timeout", first.failureMessage());

        ExecutionResult<String> second = executor.execute("retry-key", flaky);
        assertEquals(Outcome.EXECUTED, second.outcome());
        assertEquals("OK", second.value());

        ExecutionResult<String> third = executor.execute("retry-key", flaky);
        assertEquals(Outcome.REUSED, third.outcome());
        assertEquals("OK", third.value());
        assertEquals(2, attempts.get());
        assertEquals(1, executor.metrics().snapshot().failed());
    }

    @Test
    void waitForResultFalseReturnsRunningImmediately() throws Exception {
        IdempotentConfig config = IdempotentConfig.builder().waitForResult(false).build();
        IdempotentExecutor executor = new IdempotentExecutor(config);
        CountDownLatch actionEntered = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<ExecutionResult<String>> owner = pool.submit(() -> executor.execute("inflight", () -> {
                actionEntered.countDown();
                try {
                    releaseAction.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "DONE";
            }));
            assertTrue(actionEntered.await(2, TimeUnit.SECONDS));

            ExecutionResult<String> observing = executor.execute("inflight", () -> "NEVER");
            assertEquals(Outcome.RUNNING, observing.outcome());
            assertEquals(1, executor.metrics().snapshot().running());

            releaseAction.countDown();
            assertEquals(Outcome.EXECUTED, owner.get(3, TimeUnit.SECONDS).outcome());
            assertEquals(Outcome.REUSED, executor.execute("inflight", () -> "NEVER").outcome());
        } finally {
            releaseAction.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void advancingClockPastTtlAllowsReexecution() {
        MutableClock clock = new MutableClock();
        IdempotentConfig config = IdempotentConfig.builder()
                .ttlMillis(100).waitTimeoutMillis(1_000).build();
        IdempotentExecutor executor = new IdempotentExecutor(clock, config);
        AtomicInteger calls = new AtomicInteger();

        ExecutionResult<String> first = executor.execute("ttl-key", () -> "V" + calls.incrementAndGet());
        assertEquals(Outcome.EXECUTED, first.outcome());
        assertEquals("V1", first.value());
        assertEquals(Outcome.REUSED, executor.execute("ttl-key", () -> "SHOULD-NOT-RUN").outcome());

        clock.advanceMillis(101);
        ExecutionResult<String> again = executor.execute("ttl-key", () -> "V" + calls.incrementAndGet());
        assertEquals(Outcome.EXECUTED, again.outcome());
        assertEquals("V2", again.value());
        assertEquals(2, calls.get());
        assertTrue(executor.metrics().snapshot().expired() >= 1);
    }

    @Test
    void differentKeysAreIsolated() {
        IdempotentExecutor executor = new IdempotentExecutor(IdempotentConfig.defaults());
        assertEquals(Outcome.EXECUTED, executor.execute("k1", () -> "A").outcome());
        assertEquals(Outcome.EXECUTED, executor.execute("k2", () -> "B").outcome());

        assertEquals("A", executor.execute("k1", () -> "C").value());
        assertEquals("B", executor.execute("k2", () -> "C").value());
        assertEquals(Outcome.REUSED, executor.execute("k1", () -> "C").outcome());
        assertEquals(2, executor.store().size());
    }
}
