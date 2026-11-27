package dev.rainsolo.scheduler.cron;

/** Thrown when a cron expression is malformed: wrong field count, out-of-range value, zero step or unknown token. */
public class CronParseException extends IllegalArgumentException {

    public CronParseException(String message) {
        super(message);
    }

    public CronParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
