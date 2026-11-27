package dev.rainsolo.delayqueue;

import dev.rainsolo.delayqueue.config.QueueConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeWheelTest {

    static QueueConfig fastConfig() {
        return QueueConfig.builder()
                .tickMillis(1).l0Slots(64)
                .l1TickMillis(10).l1Slots(64)
                .l2TickMillis(100).l2Slots(64)
                .defaultMaxAttempts(1)
                .baseBackoffMillis(1)
                .maxBackoffMillis(2)
                .build();
    }

    @Test
    void messagesExpireInDeadlineOrderAcrossThreeLevels(@TempDir Path directory) {
        MutableClock clock = new MutableClock(1_000L);
        DelayQueueService service = DelayQueueService.manual(fastConfig(), clock, directory.resolve("s.txt"));
        List<String> fired = new ArrayList<>();
        service.registerListener("t", message -> fired.add(message.getId()));

        service.submit("m20", "t", "", 20);
        service.submit("m50", "t", "", 50);
        service.submit("m100", "t", "", 100);

        clock.advanceMillis(150);
        service.tick();

        assertEquals(List.of("m20", "m50", "m100"), fired);
        service.close();
    }

    @Test
    void overflowHeapHandlesDelaysBeyondTheHighestWheel(@TempDir Path directory) {
        MutableClock clock = new MutableClock(1_000L);
        DelayQueueService service = DelayQueueService.manual(fastConfig(), clock, directory.resolve("s.txt"));
        List<String> fired = new ArrayList<>();
        service.registerListener("t", message -> fired.add(message.getId()));

        // Level 3 covers 6.4 s with this config; 7 s must live in the overflow heap first.
        service.submit("far", "t", "", 7_000);
        clock.advanceMillis(7_100);
        service.tick();

        assertEquals(List.of("far"), fired);
        assertEquals(0, service.pendingCount());
        service.close();
    }

    @Test
    void zeroDelayMessageIsDeliveredOnNextTick(@TempDir Path directory) {
        MutableClock clock = new MutableClock(1_000L);
        DelayQueueService service = DelayQueueService.manual(fastConfig(), clock, directory.resolve("s.txt"));
        List<String> fired = new ArrayList<>();
        service.registerListener("t", message -> fired.add(message.getId()));

        service.submit("now", "t", "", 0);
        clock.advanceMillis(1);
        service.tick();

        assertTrue(fired.contains("now"));
        service.close();
    }
}
