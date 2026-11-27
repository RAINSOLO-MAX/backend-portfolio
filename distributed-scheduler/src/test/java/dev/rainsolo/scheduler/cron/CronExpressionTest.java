package dev.rainsolo.scheduler.cron;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CronExpressionTest {

    private static ZonedDateTime zdt(int year, int month, int day, int hour, int minute, int second) {
        return LocalDateTime.of(year, month, day, hour, minute, second).atZone(ZoneOffset.UTC);
    }

    @Test
    void sixFieldStepExpressionFiresEveryFifteenSeconds() {
        CronExpression cron = new CronExpression("0/15 * * * * ?");
        assertEquals("0/15 * * * * ?", cron.expression());
        ZonedDateTime base = zdt(2024, 1, 1, 0, 0, 0);
        ZonedDateTime next = cron.nextAfter(base);
        assertTrue(next.isAfter(base), "nextAfter must be strictly greater than input");
        assertEquals(zdt(2024, 1, 1, 0, 0, 15), next);
        assertEquals(zdt(2024, 1, 1, 0, 0, 30), cron.nextAfter(next));
        assertTrue(cron.matches(zdt(2024, 1, 1, 0, 0, 45)));
        assertFalse(cron.matches(zdt(2024, 1, 1, 0, 0, 46)));
        assertEquals(zdt(2024, 1, 1, 0, 1, 0), cron.nextAfter(zdt(2024, 1, 1, 0, 0, 45)));
    }

    @Test
    void sixFieldFixedSecondMinuteAndHour() {
        CronExpression cron = new CronExpression("30 15 10 * * ?");
        ZonedDateTime base = zdt(2024, 3, 5, 0, 0, 0);
        assertEquals(zdt(2024, 3, 5, 10, 15, 30), cron.nextAfter(base));
        assertTrue(cron.matches(zdt(2024, 3, 5, 10, 15, 30)));
        assertFalse(cron.matches(zdt(2024, 3, 5, 10, 15, 31)));
    }

    @Test
    void rangesListsAndStepsAreParsed() {
        CronExpression listAndRange = new CronExpression("0 5,20 1-3 * * ?");
        assertEquals(zdt(2024, 1, 1, 1, 5, 0), listAndRange.nextAfter(zdt(2024, 1, 1, 0, 0, 0)));
        assertEquals(zdt(2024, 1, 1, 1, 20, 0), listAndRange.nextAfter(zdt(2024, 1, 1, 1, 5, 0)));
        assertEquals(zdt(2024, 1, 1, 2, 5, 0), listAndRange.nextAfter(zdt(2024, 1, 1, 1, 20, 0)));

        CronExpression step = new CronExpression("0/20 * * * * ?");
        assertEquals(zdt(2024, 1, 1, 0, 0, 20), step.nextAfter(zdt(2024, 1, 1, 0, 0, 0)));
        assertEquals(zdt(2024, 1, 1, 0, 0, 40), step.nextAfter(zdt(2024, 1, 1, 0, 0, 20)));

        CronExpression rangeStep = new CronExpression("10-30/10 * * * * ?");
        assertEquals(zdt(2024, 1, 1, 0, 0, 10), rangeStep.nextAfter(zdt(2024, 1, 1, 0, 0, 0)));
        assertEquals(zdt(2024, 1, 1, 0, 0, 20), rangeStep.nextAfter(zdt(2024, 1, 1, 0, 0, 10)));
        assertEquals(zdt(2024, 1, 1, 0, 1, 10), rangeStep.nextAfter(zdt(2024, 1, 1, 0, 0, 30)));

        CronExpression dayRange = new CronExpression("0 0 0 1-5 * ?");
        assertEquals(zdt(2024, 1, 1, 0, 0, 0), dayRange.nextAfter(zdt(2023, 12, 31, 0, 0, 0)));
        assertEquals(zdt(2024, 2, 1, 0, 0, 0), dayRange.nextAfter(zdt(2024, 1, 5, 0, 0, 0)));
    }

    @Test
    void lastDayOfMonthHandlesLeapAndCommonYears() {
        CronExpression lastDay = new CronExpression("0 0 0 L * ?");
        assertEquals(zdt(2024, 2, 29, 0, 0, 0), lastDay.nextAfter(zdt(2024, 2, 10, 0, 0, 0)));
        assertEquals(zdt(2023, 2, 28, 0, 0, 0), lastDay.nextAfter(zdt(2023, 2, 10, 0, 0, 0)));
        assertEquals(zdt(2024, 3, 31, 0, 0, 0), lastDay.nextAfter(zdt(2024, 3, 1, 0, 0, 0)));
        assertTrue(lastDay.matches(zdt(2024, 2, 29, 0, 0, 0)));
        assertFalse(lastDay.matches(zdt(2024, 2, 28, 0, 0, 0)));
    }

    @Test
    void leapDayExpressionSkipsNonLeapYears() {
        CronExpression cron = new CronExpression("0 0 29 2 ?");
        ZonedDateTime next = cron.nextAfter(zdt(2023, 1, 1, 0, 0, 0));
        assertEquals(zdt(2024, 2, 29, 0, 0, 0), next);
        assertEquals(zdt(2028, 2, 29, 0, 0, 0), cron.nextAfter(zdt(2025, 3, 1, 0, 0, 0)));
        assertTrue(cron.matches(zdt(2024, 2, 29, 0, 0, 0)));
    }

    @Test
    void yearRolloverReturnsJanuaryFirst() {
        CronExpression cron = new CronExpression("0 0 1 1 ?");
        ZonedDateTime next = cron.nextAfter(zdt(2023, 6, 15, 12, 30, 0));
        assertEquals(zdt(2024, 1, 1, 0, 0, 0), next);
        assertTrue(next.isAfter(zdt(2023, 6, 15, 12, 30, 0)));
    }

    @Test
    void weekdaySevenAndZeroBothMeanSunday() {
        ZonedDateTime monday = zdt(2024, 1, 1, 0, 0, 0);
        assertEquals(zdt(2024, 1, 7, 0, 0, 0), new CronExpression("0 0 0 ? * 7").nextAfter(monday));
        assertEquals(zdt(2024, 1, 7, 0, 0, 0), new CronExpression("0 0 0 ? * 0").nextAfter(monday));
        assertTrue(new CronExpression("0 0 0 ? * 7").matches(zdt(2024, 1, 7, 0, 0, 0)));
    }

    @Test
    void invalidExpressionsThrowCronParseException() {
        assertThrows(CronParseException.class, () -> new CronExpression("* * * *"));
        assertThrows(CronParseException.class, () -> new CronExpression("* * * * * * *"));
        assertThrows(CronParseException.class, () -> new CronExpression("0 0 0 32 * ?"));
        assertThrows(CronParseException.class, () -> new CronExpression("60 * * * * ?"));
        assertThrows(CronParseException.class, () -> new CronExpression("0 0 0 * 13 ?"));
        assertThrows(CronParseException.class, () -> new CronExpression("0 0 0 * * 8"));
        assertThrows(CronParseException.class, () -> new CronExpression("0/0 * * * * ?"));
        assertThrows(CronParseException.class, () -> new CronExpression("X * * * * ?"));
        assertThrows(CronParseException.class, () -> new CronExpression("0 0 0 L-1 * ?"));
        assertThrows(CronParseException.class, () -> new CronExpression("0 0 0 5-1 * ?"));
    }
}
