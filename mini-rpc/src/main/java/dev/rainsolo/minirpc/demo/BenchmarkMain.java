package dev.rainsolo.minirpc.demo;

import dev.rainsolo.minirpc.client.RpcClient;
import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.registry.InMemoryRegistry;
import dev.rainsolo.minirpc.server.RpcServer;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/** Reproducible load test; prints QPS and latency percentiles used in the README. */
public final class BenchmarkMain {

    private static final int THREADS = 16;
    private static final int CALLS_PER_THREAD = 2_000;
    private static final int WARMUP = 2_000;

    public static void main(String[] args) throws Exception {
        InMemoryRegistry registry = new InMemoryRegistry();
        RpcServer server = new RpcServer(0, RpcConfig.defaults(), registry);
        server.registerService(DemoService.class, new DemoServiceImpl());
        server.start();

        try (RpcClient client = new RpcClient(RpcConfig.defaults(), registry)) {
            DemoService service = client.createProxy(DemoService.class);
            for (int i = 0; i < WARMUP; i++) {
                service.echo("warmup");
            }

            List<Long> latencies = new ArrayList<>(THREADS * CALLS_PER_THREAD);
            AtomicInteger failures = new AtomicInteger();
            ExecutorService pool = Executors.newFixedThreadPool(THREADS);
            CountDownLatch latch = new CountDownLatch(THREADS);
            long start = System.nanoTime();

            for (int t = 0; t < THREADS; t++) {
                pool.submit(() -> {
                    List<Long> local = new ArrayList<>(CALLS_PER_THREAD);
                    for (int i = 0; i < CALLS_PER_THREAD; i++) {
                        long begin = System.nanoTime();
                        try {
                            service.echo("bench");
                            local.add(System.nanoTime() - begin);
                        } catch (RuntimeException e) {
                            failures.incrementAndGet();
                        }
                    }
                    synchronized (latencies) {
                        latencies.addAll(local);
                    }
                    latch.countDown();
                });
            }
            latch.await();
            long elapsedNanos = System.nanoTime() - start;
            pool.shutdownNow();

            double seconds = elapsedNanos / 1_000_000_000.0;
            long qps = Math.round((THREADS * CALLS_PER_THREAD) / seconds);
            long[] values = latencies.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(values);

            System.out.println("---- mini-rpc benchmark ----");
            System.out.println("threads            : " + THREADS);
            System.out.println("requests           : " + THREADS * CALLS_PER_THREAD);
            System.out.println("elapsed            : " + String.format("%.2f", seconds) + " s");
            System.out.println("throughput         : " + qps + " req/s");
            System.out.println("failures           : " + failures.get());
            System.out.println("p50 / p95 / p99 ms : "
                    + String.format("%.2f / %.2f / %.2f",
                    percentile(values, 50) / 1_000_000.0,
                    percentile(values, 95) / 1_000_000.0,
                    percentile(values, 99) / 1_000_000.0));
        } finally {
            server.close();
            registry.close();
        }
    }

    private static long percentile(long[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(sorted.length * p / 100.0) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }
}
