package dev.rainsolo.scheduler.job;

/** Policy that decides how many missed fire cycles are compensated after a downtime. */
public enum MisfirePolicy {

    /** Drop every missed cycle and simply resume from the next scheduled time. */
    IGNORE,

    /** Fire exactly one compensation for all missed cycles. */
    FIRE_ONCE_NOW,

    /** Do not compensate; advance the last-fire marker and wait for the next cycle. */
    RESCHEDULE_NEXT
}
