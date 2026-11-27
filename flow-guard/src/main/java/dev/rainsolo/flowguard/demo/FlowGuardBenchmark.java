package dev.rainsolo.flowguard.demo;

import dev.rainsolo.flowguard.core.LimiterConfig;
import dev.rainsolo.flowguard.core.SystemClock;
import dev.rainsolo.flowguard.limiter.RateLimiter;
import dev.rainsolo.flowguard.spi.Limiters;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

/** Measures throughput and p99 latency for the four algorithms under 16 concurrent callers. */
public final class FlowGuardBenchmark {

    private static final int THREADS = 16;
    private static final int ITERATIONS = 10_000;
    private static final int WARMUP = 2_000;
    private static final String[] ALGORITHMS = {"fixed", "sliding", "leaky", "token"};

    public static void main(String[] args) throws InterruptedException {
        System.out.println("---- flow-guard benchmark ----");
        System.out.printf(Locale.ROOT, "threads=%d iterations/thread=%d warmup=%d total=%d%n",
                THREADS, ITERATIONS, WARMUP, THREADS * ITERATIONS);
        LimiterConfig config = LimiterConfig.builder()
                .permitsPerSecond(1_000_000).burstCapacity(1_000_000)
                .windowMillis(1_000).slidingSlots(10).build();
        for (String algorithm : ALGORITHMS) {
            Result result = measure(algorithm, config);
            System.out.printf(Locale.ROOT, "%-8s ops/s=%,12.0f  p99=%8.2f us%n",
                    algorithm, result.opsPerSecond(), result.p99Micros());
        }
    }

    private static Result measure(String algorithm, LimiterConfig config) throws InterruptedException {
        RateLimiter limiter = Limiters.create(algorithm, config, new SystemClock());
        for (int i = 0; i < WARMUP; i++) {
            limiter.tryAcquire("bench", 1);
        }
        long[] samples = new long[THREADS * ITERATIONS];
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        Thread[] threads = new Thread[THREADS];
        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                int base = threadId * ITERATIONS;
                try {
                    start.await();
                    for (int i = 0; i < ITERATIONS; i++) {
                        long begin = System.nanoTime();
                        limiter.tryAcquire("bench", 1);
                        samples[base + i] = System.nanoTime() - begin;
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            threads[t].start();
        }
        long beginNanos = System.nanoTime();
        start.countDown();
        done.await();
        double seconds = (System.nanoTime() - beginNanos) / 1_000_000_000.0;
        Arrays.sort(samples);
        int index = (int) Math.ceil(samples.length * 99 / 100.0) - 1;
        long p99Nanos = samples[Math.max(0, Math.min(index, samples.length - 1))];
        return new Result(THREADS * ITERATIONS / seconds, p99Nanos / 1_000.0);
    }

    private record Result(double opsPerSecond, double p99Micros) {
    }
}
