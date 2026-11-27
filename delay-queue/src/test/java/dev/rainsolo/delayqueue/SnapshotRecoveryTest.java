package dev.rainsolo.delayqueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SnapshotRecoveryTest {

    @Test
    void onlyUnfinishedMessagesAreSnapshottedAndRestored(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("snapshot.txt");
        MutableClock firstClock = new MutableClock(1_700_000_000_000L);
        DelayQueueService first = DelayQueueService.manual(TimeWheelTest.fastConfig(), firstClock, file);
        List<String> fired = new ArrayList<>();
        first.registerListener("t", message -> fired.add(message.getId()));

        first.submit("done", "t", "payload", 10);
        first.submit("pending", "t", "payload", 5_000);
        firstClock.advanceMillis(100);
        first.tick();
        assertEquals(List.of("done"), fired);

        first.snapshot();
        assertEquals(1, Files.readAllLines(file).size(), "finished messages must not be persisted");
        first.close();

        // A new JVM-like instance with a completely different wall clock.
        MutableClock secondClock = new MutableClock(1_000L);
        DelayQueueService second = DelayQueueService.manual(TimeWheelTest.fastConfig(), secondClock, file);
        List<String> recovered = new ArrayList<>();
        second.registerListener("t", message -> recovered.add(message.getId()));

        assertEquals(1, second.restore());
        secondClock.advanceMillis(5_100);
        second.tick();

        assertEquals(List.of("pending"), recovered);
        assertEquals(1, second.metrics().succeeded());
        second.close();
    }

    @Test
    void deliveringMessageIsReplayedAfterCrash(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("snapshot.txt");
        MutableClock clock = new MutableClock(1_000L);
        DelayQueueService service = DelayQueueService.manual(TimeWheelTest.fastConfig(), clock, file);
        service.submit("in-flight", "t", "payload", 500);
        service.snapshot();
        service.close();

        MutableClock restarted = new MutableClock(2_000L);
        DelayQueueService revived = DelayQueueService.manual(TimeWheelTest.fastConfig(), restarted, file);
        List<String> fired = new ArrayList<>();
        revived.registerListener("t", message -> fired.add(message.getId()));

        assertEquals(1, revived.restore());
        restarted.advanceMillis(600);
        revived.tick();
        assertEquals(List.of("in-flight"), fired);
        revived.close();
    }
}
