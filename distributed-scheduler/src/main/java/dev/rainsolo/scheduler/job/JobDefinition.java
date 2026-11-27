package dev.rainsolo.scheduler.job;

import dev.rainsolo.scheduler.cron.CronExpression;

import java.util.List;
import java.util.Objects;

/** Immutable description of a scheduled job: cron, shard count, shard parameters and runtime policies. */
public final class JobDefinition {

    private final String jobName;
    private final CronExpression cron;
    private final int shardingTotalCount;
    private final List<String> shardParameters;
    private final MisfirePolicy misfirePolicy;
    private final long timeoutMillis;
    private final boolean failoverEnabled;

    private JobDefinition(Builder builder) {
        this.jobName = Objects.requireNonNull(builder.jobName, "jobName");
        this.cron = Objects.requireNonNull(builder.cron, "cron");
        if (this.jobName.isBlank()) {
            throw new IllegalArgumentException("jobName must not be blank");
        }
        if (builder.shardingTotalCount <= 0) {
            throw new IllegalArgumentException("shardingTotalCount must be positive");
        }
        if (builder.timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis must not be negative");
        }
        this.shardingTotalCount = builder.shardingTotalCount;
        this.shardParameters = List.copyOf(builder.shardParameters);
        this.misfirePolicy = Objects.requireNonNull(builder.misfirePolicy, "misfirePolicy");
        this.timeoutMillis = builder.timeoutMillis;
        this.failoverEnabled = builder.failoverEnabled;
    }

    /** Returns a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    public String jobName() {
        return jobName;
    }

    public CronExpression cron() {
        return cron;
    }

    public int shardingTotalCount() {
        return shardingTotalCount;
    }

    public List<String> shardParameters() {
        return shardParameters;
    }

    public MisfirePolicy misfirePolicy() {
        return misfirePolicy;
    }

    public long timeoutMillis() {
        return timeoutMillis;
    }

    public boolean failoverEnabled() {
        return failoverEnabled;
    }

    /** Returns the parameter bound to a shard item, reusing parameters cyclically when the list is shorter. */
    public String shardParameter(int shardItem) {
        if (shardParameters.isEmpty()) {
            return "";
        }
        return shardParameters.get(Math.floorMod(shardItem, shardParameters.size()));
    }

    /** Fluent builder with safe defaults. */
    public static final class Builder {
        private String jobName;
        private CronExpression cron;
        private int shardingTotalCount = 1;
        private List<String> shardParameters = List.of();
        private MisfirePolicy misfirePolicy = MisfirePolicy.IGNORE;
        private long timeoutMillis = 0L;
        private boolean failoverEnabled = true;

        public Builder jobName(String value) {
            this.jobName = value;
            return this;
        }

        public Builder cron(CronExpression value) {
            this.cron = value;
            return this;
        }

        public Builder cron(String expression) {
            this.cron = new CronExpression(expression);
            return this;
        }

        public Builder shardingTotalCount(int value) {
            this.shardingTotalCount = value;
            return this;
        }

        public Builder shardParameters(List<String> value) {
            this.shardParameters = value;
            return this;
        }

        public Builder misfirePolicy(MisfirePolicy value) {
            this.misfirePolicy = value;
            return this;
        }

        public Builder timeoutMillis(long value) {
            this.timeoutMillis = value;
            return this;
        }

        public Builder failoverEnabled(boolean value) {
            this.failoverEnabled = value;
            return this;
        }

        public JobDefinition build() {
            return new JobDefinition(this);
        }
    }
}
