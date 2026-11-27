package dev.rainsolo.delayqueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClockSkewTest {

    @Test
    void wallClockRewindDoesNotChangeDelivery(@TempDir Path directory) {
        long initialWall = 1_700_000_000_000L;
        MutableClock clock = new MutableClock(initialWall);
        DelayQueueService service = DelayQueueService.manual(TimeWheelTest.fastConfig(), clock,
                directory.resolve("s.txt"));
        List<String> fired = new ArrayList<>();
        service.registerListener("t", message -> fired.add(message.getId()));

        service.submit("before-rewind", "t", "", 50);
        clock.setWall(initialWall - 3_600_000L);
        clock.advanceMillis(60);
        service.tick();

        assertEquals(List.of("before-rewind"), fired);

        clock.setWall(initialWall + 3_600_000L);
        service.submit("after-jump", "t", "", 30);
        clock.advanceMillis(40);
        service.tick();

        assertEquals(2, fired.size());
        assertEquals("after-jump", fired.get(1));
        service.close();
    }
}
