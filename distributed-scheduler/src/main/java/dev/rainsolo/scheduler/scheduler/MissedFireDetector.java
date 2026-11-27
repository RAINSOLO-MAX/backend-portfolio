package dev.rainsolo.scheduler.scheduler;

import dev.rainsolo.scheduler.cron.CronExpression;
import dev.rainsolo.scheduler.job.MisfirePolicy;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes compensation for cycles missed between the last fire time and now.
 * A single elapsed cycle is a normal trigger; two or more elapsed cycles are a
 * misfire and are resolved according to the job's {@link MisfirePolicy}.
 * The last-fire marker always advances to the most recent due fire time.
 */
public final class MissedFireDetector {

    /** Detection result: fire times to execute, the new last-fire marker and compensated cycles. */
    public record Result(List<Long> fireTimes, long updatedLastFireMillis, long misfired) {
    }

    private static final int MAX_CYCLES = 10_000;

    private final ZoneId zone;

    public MissedFireDetector(ZoneId zone) {
        this.zone = zone;
    }

    /**
     * Enumerates due fire times strictly after {@code lastFireMillis} and not after
     * {@code nowMillis}, then applies the misfire policy when more than one cycle elapsed.
     */
    public Result detect(CronExpression cron, MisfirePolicy policy, long lastFireMillis, long nowMillis) {
        List<Long> due = new ArrayList<>();
        ZonedDateTime cursor = Instant.ofEpochMilli(lastFireMillis).atZone(zone);
        while (due.size() < MAX_CYCLES) {
            ZonedDateTime next = cron.nextAfter(cursor);
            long fireTime = next.toInstant().toEpochMilli();
            if (fireTime > nowMillis) {
                break;
            }
            due.add(fireTime);
            cursor = next;
        }
        if (due.isEmpty()) {
            return new Result(List.of(), lastFireMillis, 0L);
        }
        long lastDue = due.get(due.size() - 1);
        if (due.size() == 1) {
            return new Result(List.of(lastDue), lastDue, 0L);
        }
        if (policy == MisfirePolicy.FIRE_ONCE_NOW) {
            return new Result(List.of(lastDue), lastDue, 1L);
        }
        return new Result(List.of(), lastDue, 0L);
    }
}
