package dev.rainsolo.delayqueue.demo;

import dev.rainsolo.delayqueue.DelayQueueService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** End-to-end demo: retry, dead letter, idempotency, snapshot, restart and restore. */
public final class DelayQueueDemo {

    public static void main(String[] args) throws Exception {
        Path file = Path.of(System.getProperty("java.io.tmpdir"), "delay-queue-demo.snapshot");
        Files.deleteIfExists(file);

        Map<String, AtomicInteger> attemptsByMessage = new ConcurrentHashMap<>();
        DelayQueueService service = DelayQueueService.create(file);
        service.registerListener("order", message -> {
            int attempt = attemptsByMessage
                    .computeIfAbsent(message.getId(), ignored -> new AtomicInteger())
                    .incrementAndGet();
            System.out.println("[order]  " + message.getId() + " attempt=" + attempt);
            if (message.getId().equals("order-1") && attempt <= 2) {
                throw new IOException("transient downstream failure");
            }
            if (message.getId().equals("poison")) {
                throw new IOException("permanent failure");
            }
        });
        service.registerListener("alarm", message ->
                System.out.println("[alarm]  " + message.getId() + " -> " + message.getPayload()));

        service.submit("alarm-1", "alarm", "wake up", 80);
        service.submit("order-1", "order", "cancel-if-unpaid", 120);
        service.submit("poison", "order", "always-fails", 60, 1);
        service.submit("duplicate", "alarm", "same-id", 40);
        service.submit("duplicate", "alarm", "same-id", 40);
        service.submit("survives-restart", "alarm", "restored-after-crash", 3_000);

        Thread.sleep(700);
        service.snapshot();
        System.out.println("\nmetrics after first run: " + service.metrics());
        System.out.println("dead letters            : " + service.deadLetters().size());
        service.close();

        DelayQueueService revived = DelayQueueService.create(file);
        revived.registerListener("alarm", message ->
                System.out.println("[revived]" + message.getId() + " -> " + message.getPayload()));
        int restored = revived.restore();
        System.out.println("\nrestored messages       : " + restored);
        Thread.sleep(3_200);
        System.out.println("metrics after restore   : " + revived.metrics());
        revived.close();
    }
}
