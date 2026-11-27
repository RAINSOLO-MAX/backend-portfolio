package dev.rainsolo.scheduler.cron;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.BitSet;

/**
 * A hand-written cron parser supporting 5 fields (minute hour day month weekday) and
 * 6 fields (second minute hour day month weekday).
 *
 * <p>Each field accepts {@code *}, {@code ,}, {@code -}, {@code /}, {@code ?} and,
 * for the day-of-month field only, {@code L} (last day of the month). Weekday 7 is
 * normalised to 0 so both 0 and 7 mean Sunday. Matching values are held in bit sets.
 *
 * <p>{@link #nextAfter(ZonedDateTime)} advances field by field with carry instead of
 * scanning every second, and is guarded against endless loops (for example an
 * impossible date). It correctly handles leap years, month rollover and year rollover.
 */
public final class CronExpression {

    /** Upper bound on field-advance iterations; also bounds the search to eight years. */
    private static final int MAX_ITERATIONS = 500_000;
    private static final int MAX_YEARS_AHEAD = 8;

    private final String expression;
    private final BitSet seconds;
    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet daysOfMonth;
    private final BitSet months;
    private final BitSet daysOfWeek;
    private final boolean lastDayOfMonth;
    private final boolean dayOfMonthAny;
    private final boolean dayOfWeekAny;

    /**
     * Parses the given cron expression.
     *
     * @param expression 5-field or 6-field cron expression
     * @throws CronParseException if the expression is malformed
     */
    public CronExpression(String expression) {
        if (expression == null) {
            throw new CronParseException("Cron expression must not be null");
        }
        this.expression = expression.trim();
        String[] parts = this.expression.split("\\s+");
        if (parts.length != 5 && parts.length != 6) {
            throw new CronParseException("Cron expression must have 5 or 6 fields, got " + parts.length
                    + ": " + this.expression);
        }
        String secField;
        String minField;
        String hourField;
        String domField;
        String monthField;
        String dowField;
        if (parts.length == 6) {
            secField = parts[0];
            minField = parts[1];
            hourField = parts[2];
            domField = parts[3];
            monthField = parts[4];
            dowField = parts[5];
        } else {
            secField = "0";
            minField = parts[0];
            hourField = parts[1];
            domField = parts[2];
            monthField = parts[3];
            dowField = parts[4];
        }

        boolean[] domAny = new boolean[1];
        boolean[] dowAny = new boolean[1];
        this.seconds = parseField(secField, 0, 59, "second", new boolean[1]);
        this.minutes = parseField(minField, 0, 59, "minute", new boolean[1]);
        this.hours = parseField(hourField, 0, 23, "hour", new boolean[1]);
        this.months = parseField(monthField, 1, 12, "month", new boolean[1]);
        this.daysOfWeek = parseField(dowField, 0, 7, "weekday", dowAny);
        if (this.daysOfWeek.get(7)) {
            this.daysOfWeek.set(0);
        }
        this.dayOfWeekAny = dowAny[0];

        if ("L".equals(domField.trim())) {
            this.lastDayOfMonth = true;
            this.daysOfMonth = new BitSet(32);
            this.dayOfMonthAny = false;
        } else {
            this.lastDayOfMonth = false;
            this.daysOfMonth = parseField(domField, 1, 31, "day", domAny);
            this.dayOfMonthAny = domAny[0];
        }
    }

    /** Convenience factory for a parsed cron expression. */
    public static CronExpression of(String expression) {
        return new CronExpression(expression);
    }

    /** Returns the original expression text. */
    public String expression() {
        return expression;
    }

    /** Returns true when the supplied instant lands exactly on a matching second. */
    public boolean matches(ZonedDateTime time) {
        ZonedDateTime t = time.withNano(0);
        return seconds.get(t.getSecond())
                && minutes.get(t.getMinute())
                && hours.get(t.getHour())
                && months.get(t.getMonthValue())
                && dayMatches(t);
    }

    /**
     * Returns the first matching time strictly after {@code after}, aligned to whole seconds.
     * Uses field-by-field advancement with carry rather than scanning every second.
     *
     * @throws CronParseException if no matching time can be found within eight years
     */
    public ZonedDateTime nextAfter(ZonedDateTime after) {
        ZonedDateTime t = after.withNano(0).plusSeconds(1);
        int limitYear = after.getYear() + MAX_YEARS_AHEAD;
        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            if (t.getYear() > limitYear) {
                throw new CronParseException("No next fire time within " + MAX_YEARS_AHEAD
                        + " years for " + expression);
            }
            if (!months.get(t.getMonthValue())) {
                t = t.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
                continue;
            }
            if (!dayMatches(t)) {
                t = t.plusDays(1).withHour(0).withMinute(0).withSecond(0);
                continue;
            }
            if (!hours.get(t.getHour())) {
                t = t.plusHours(1).withMinute(0).withSecond(0);
                continue;
            }
            if (!minutes.get(t.getMinute())) {
                t = t.plusMinutes(1).withSecond(0);
                continue;
            }
            if (!seconds.get(t.getSecond())) {
                t = t.plusSeconds(1);
                continue;
            }
            return t;
        }
        throw new CronParseException("Field advancement exceeded " + MAX_ITERATIONS
                + " iterations for " + expression);
    }

    /** Returns the first matching epoch millisecond strictly after the supplied instant. */
    public long nextAfterMillis(long afterMillis, java.time.ZoneId zone) {
        ZonedDateTime base = Instant.ofEpochMilli(afterMillis).atZone(zone);
        return nextAfter(base).toInstant().toEpochMilli();
    }

    private boolean dayMatches(ZonedDateTime t) {
        boolean domOk = lastDayOfMonth
                ? t.getDayOfMonth() == t.toLocalDate().lengthOfMonth()
                : daysOfMonth.get(t.getDayOfMonth());
        int cronDow = t.getDayOfWeek().getValue() % 7;
        boolean dowOk = daysOfWeek.get(cronDow);
        if (dayOfMonthAny && dayOfWeekAny) {
            return true;
        }
        if (dayOfMonthAny) {
            return dowOk;
        }
        if (dayOfWeekAny) {
            return domOk;
        }
        return domOk && dowOk;
    }

    private static BitSet parseField(String field, int min, int max, String label, boolean[] anyFlag) {
        BitSet bits = new BitSet(max + 1);
        String text = field.trim();
        if (text.isEmpty()) {
            throw new CronParseException("Empty " + label + " field");
        }
        if ("?".equals(text)) {
            anyFlag[0] = true;
            bits.set(min, max + 1);
            return bits;
        }
        String[] items = text.split(",", -1);
        for (String item : items) {
            if (item.isEmpty()) {
                throw new CronParseException("Empty item in " + label + " field: " + text);
            }
            String[] slash = item.split("/", -1);
            if (slash.length > 2) {
                throw new CronParseException("Invalid step in " + label + " field: " + item);
            }
            int step = 1;
            if (slash.length == 2) {
                if (!isDigits(slash[1])) {
                    throw new CronParseException("Invalid step value in " + label + " field: " + item);
                }
                step = Integer.parseInt(slash[1]);
                if (step <= 0) {
                    throw new CronParseException("Step must be positive (got /" + slash[1]
                            + ") in " + label + " field");
                }
            }
            String range = slash[0];
            int from;
            int to;
            if ("*".equals(range)) {
                from = min;
                to = max;
            } else if (range.indexOf('-') >= 0) {
                String[] bounds = range.split("-", -1);
                if (bounds.length != 2) {
                    throw new CronParseException("Invalid range in " + label + " field: " + item);
                }
                from = parseNumber(bounds[0], min, max, label);
                to = parseNumber(bounds[1], min, max, label);
                if (from > to) {
                    throw new CronParseException("Range start exceeds end in " + label + " field: " + item);
                }
            } else {
                from = parseNumber(range, min, max, label);
                to = slash.length == 2 ? max : from;
            }
            for (int value = from; value <= to; value += step) {
                bits.set(value);
            }
        }
        return bits;
    }

    private static int parseNumber(String token, int min, int max, String label) {
        if (!isDigits(token)) {
            throw new CronParseException("Unknown token '" + token + "' in " + label + " field");
        }
        int value = Integer.parseInt(token);
        if (value < min || value > max) {
            throw new CronParseException("Value " + value + " out of range [" + min + ", " + max
                    + "] in " + label + " field");
        }
        return value;
    }

    private static boolean isDigits(String token) {
        if (token.isEmpty()) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            if (token.charAt(i) < '0' || token.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }
}
