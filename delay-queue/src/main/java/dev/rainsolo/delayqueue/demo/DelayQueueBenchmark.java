package dev.rainsolo.delayqueue.demo;

import dev.rainsolo.delayqueue.DelayQueueService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Measures dispatch throughput and scheduling drift (actual fire time minus deadline). */
public final class DelayQueueBenchmark {

    private static final int MESSAGES = 10_000;

    public static void main(String[] args) throws Exception {
        Path file = Path.of(System.getProperty("java.io.tmpdir"), "delay-queue-bench.snapshot");
        DelayQueueService service = DelayQueueService.create(file);
        List<Long> drifts = new ArrayList<>(MESSAGES);
        CountDownLatch latch = new CountDownLatch(MESSAGES);

        service.registerListener("bench", message -> {
            synchronized (drifts) {
                drifts.add(System.nanoTime() - message.getDeadlineNanos());
            }
            latch.countDown();
        });

        long start = System.nanoTime();
        for (int i = 0; i < MESSAGES; i++) {
            service.submit("bench-" + i, "bench", "payload", 10 + (i % 190));
        }
        boolean completed = latch.await(20, TimeUnit.SECONDS);
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;

        long[] values;
        synchronized (drifts) {
            values = drifts.stream().mapToLong(Long::longValue).toArray();
        }
        Arrays.sort(values);

        System.out.println("---- delay-queue benchmark ----");
        System.out.println("messages           : " + MESSAGES);
        System.out.println("completed          : " + completed);
        System.out.println("elapsed            : " + String.format("%.2f", seconds) + " s");
        System.out.println("dispatch throughput: " + Math.round(MESSAGES / seconds) + " msg/s");
        System.out.println("scheduling drift   : p50 " + String.format("%.2f", percentile(values, 50) / 1_000_000.0)
                + " ms, p95 " + String.format("%.2f", percentile(values, 95) / 1_000_000.0)
                + " ms, p99 " + String.format("%.2f", percentile(values, 99) / 1_000_000.0) + " ms");
        service.close();
    }

    private static long percentile(long[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(sorted.length * p / 100.0) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }
}
