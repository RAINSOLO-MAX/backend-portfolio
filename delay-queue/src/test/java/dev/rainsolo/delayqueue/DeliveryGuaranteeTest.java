package dev.rainsolo.delayqueue;

import dev.rainsolo.delayqueue.config.QueueConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeliveryGuaranteeTest {

    private static QueueConfig config() {
        return TimeWheelTest.fastConfig();
    }

    private static void advance(DelayQueueService service, MutableClock clock, int rounds) {
        for (int i = 0; i < rounds; i++) {
            clock.advanceMillis(20);
            service.tick();
        }
    }

    @Test
    void failedDeliveryRetriesWithBackoffThenGoesToDeadLetter(@TempDir Path directory) {
        MutableClock clock = new MutableClock(1_000L);
        DelayQueueService service = DelayQueueService.manual(config(), clock, directory.resolve("s.txt"));
        AtomicInteger calls = new AtomicInteger();
        service.registerListener("flaky", message -> {
            calls.incrementAndGet();
            throw new IOException("downstream unavailable");
        });

        service.submit("m1", "flaky", "", 1, 3);
        advance(service, clock, 10);

        assertEquals(3, calls.get());
        assertEquals(2, service.metrics().retried());
        assertEquals(1, service.metrics().deadLettered());
        assertEquals(1, service.deadLetters().size());
        assertEquals("m1", service.deadLetters().list().get(0).getId());
        service.close();
    }

    @Test
    void duplicateMessageIdIsOnlyProcessedOnce(@TempDir Path directory) {
        MutableClock clock = new MutableClock(1_000L);
        DelayQueueService service = DelayQueueService.manual(config(), clock, directory.resolve("s.txt"));
        AtomicInteger calls = new AtomicInteger();
        service.registerListener("t", message -> calls.incrementAndGet());

        service.submit("dup", "t", "", 10);
        service.submit("dup", "t", "", 10);
        advance(service, clock, 2);

        assertEquals(1, calls.get());
        assertEquals(1, service.metrics().duplicatesSuppressed());
        assertEquals(1, service.metrics().succeeded());
        service.close();
    }

    @Test
    void successfulDeliveryIsNeverRetried(@TempDir Path directory) {
        MutableClock clock = new MutableClock(1_000L);
        DelayQueueService service = DelayQueueService.manual(config(), clock, directory.resolve("s.txt"));
        service.registerListener("t", message -> {
        });

        service.submit("ok", "t", "", 5);
        advance(service, clock, 2);

        assertEquals(1, service.metrics().succeeded());
        assertEquals(0, service.metrics().retried());
        assertEquals(0, service.deadLetters().size());
        service.close();
    }
}
