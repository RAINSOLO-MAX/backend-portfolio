package dev.rainsolo.idemlock.demo;

import dev.rainsolo.idemlock.core.IdempotentConfig;
import dev.rainsolo.idemlock.executor.IdempotentExecutor;
import dev.rainsolo.idemlock.lock.DistributedLock;
import dev.rainsolo.idemlock.lock.LockManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Multi-threaded throughput and p99 latency benchmark, using nanoTime samples and Arrays.sort. */
public final class IdempotentLockBenchmark {

    private static final int THREADS = 16;
    private static final int OPS_PER_THREAD = 20_000;

    private IdempotentLockBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        runIdempotent(4, 2_000);
        runLock(4, 2_000);

        Bench idempotent = runIdempotent(THREADS, OPS_PER_THREAD);
        Bench lock = runLock(THREADS, OPS_PER_THREAD);

        System.out.println("=== BENCHMARK threads=" + THREADS + " opsPerThread=" + OPS_PER_THREAD + " ===");
        print("IDEMPOTENT_EXECUTE", idempotent);
        print("LOCK_ACQUIRE_RELEASE", lock);
    }

    private static void print(String label, Bench bench) {
        System.out.printf(Locale.ROOT,
                "%s ops=%d elapsedMs=%d throughput=%.0f ops/s p99Us=%.1f%n",
                label, bench.operations(), bench.elapsedMillis(),
                bench.opsPerSecond(), bench.p99Micros());
    }

    private static Bench runIdempotent(int threads, int opsPerThread) throws Exception {
        IdempotentExecutor executor = new IdempotentExecutor(IdempotentConfig.defaults());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<long[]>> futures = new ArrayList<>();
        long start = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                long[] samples = new long[opsPerThread];
                for (int i = 0; i < opsPerThread; i++) {
                    long before = System.nanoTime();
                    executor.execute("bench-" + id + "-" + i, () -> "value");
                    samples[i] = System.nanoTime() - before;
                }
                return samples;
            }));
        }
        long[] samples = collect(futures, threads * opsPerThread);
        long elapsed = System.nanoTime() - start;
        shutdown(pool);
        return bench(samples, elapsed);
    }

    private static Bench runLock(int threads, int opsPerThread) throws Exception {
        LockManager manager = LockManager.builder()
                .defaultLeaseMillis(5_000).watchdog(false).build();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<long[]>> futures = new ArrayList<>();
        long start = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                long[] samples = new long[opsPerThread];
                for (int i = 0; i < opsPerThread; i++) {
                    long before = System.nanoTime();
                    DistributedLock lock = manager.lock("bench-lock-" + id);
                    if (lock.tryLock(5_000)) {
                        lock.unlock();
                    }
                    samples[i] = System.nanoTime() - before;
                }
                return samples;
            }));
        }
        long[] samples = collect(futures, threads * opsPerThread);
        long elapsed = System.nanoTime() - start;
        shutdown(pool);
        return bench(samples, elapsed);
    }

    private static long[] collect(List<Future<long[]>> futures, int total) throws Exception {
        long[] all = new long[total];
        int offset = 0;
        for (Future<long[]> future : futures) {
            long[] part = future.get(60, TimeUnit.SECONDS);
            System.arraycopy(part, 0, all, offset, part.length);
            offset += part.length;
        }
        return all;
    }

    private static Bench bench(long[] samples, long elapsedNanos) {
        Arrays.sort(samples);
        int p99Index = Math.min(samples.length - 1, (int) (samples.length * 0.99));
        return new Bench(samples.length, elapsedNanos, samples[p99Index]);
    }

    private static void shutdown(ExecutorService pool) throws InterruptedException {
        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
    }

    /** Aggregated result of one benchmark round. */
    private record Bench(long operations, long elapsedNanos, long p99Nanos) {

        double opsPerSecond() {
            return operations * 1_000_000_000.0 / elapsedNanos;
        }

        double p99Micros() {
            return p99Nanos / 1_000.0;
        }

        long elapsedMillis() {
            return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        }
    }
}
