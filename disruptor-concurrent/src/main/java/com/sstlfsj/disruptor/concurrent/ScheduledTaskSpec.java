package com.sstlfsj.disruptor.concurrent;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.util.Objects;

/** 不可变、类型安全的高级调度定义。 */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public final class ScheduledTaskSpec<V> {

    private final ContextCallable<V> task;
    private final TaskContext context;
    private final Duration triggerAfter;
    private final ScheduleMode scheduleMode;
    private final Duration period;
    private final DynamicDelay dynamicDelay;
    private final Duration expiresAfter;
    private final Integer maxExecutions;
    private final boolean continueOnFailure;
    private final int priority;
    private final CancellationToken cancellationToken;

    @Builder(toBuilder = true)
    private ScheduledTaskSpec(
            ContextCallable<V> task,
            TaskContext context,
            Duration triggerAfter,
            ScheduleMode scheduleMode,
            Duration period,
            DynamicDelay dynamicDelay,
            Duration expiresAfter,
            Integer maxExecutions,
            boolean continueOnFailure,
            int priority,
            CancellationToken cancellationToken) {
        this.task = Objects.requireNonNull(task, "task 不能为空");
        this.context = context == null ? TaskContext.empty() : context;
        this.triggerAfter = triggerAfter == null ? Duration.ZERO : requireNonNegative(
                triggerAfter, "triggerAfter");
        this.scheduleMode = scheduleMode == null ? ScheduleMode.ONE_SHOT : scheduleMode;
        this.expiresAfter = expiresAfter == null ? null : requireNonNegative(
                expiresAfter, "expiresAfter");
        if (maxExecutions != null && maxExecutions <= 0) {
            throw new IllegalArgumentException("maxExecutions 必须为正数，实际值=" + maxExecutions);
        }
        this.maxExecutions = maxExecutions;
        this.continueOnFailure = continueOnFailure;
        this.priority = priority;
        this.cancellationToken = cancellationToken == null
                ? CancellationToken.none()
                : cancellationToken;

        Duration resolvedPeriod;
        DynamicDelay resolvedDynamicDelay;
        switch (this.scheduleMode) {
            case ONE_SHOT -> {
                if (period != null || dynamicDelay != null) {
                    throw new IllegalArgumentException("ONE_SHOT 不能配置 period 或 dynamicDelay");
                }
                resolvedPeriod = null;
                resolvedDynamicDelay = null;
            }
            case FIXED_RATE, FIXED_DELAY -> {
                resolvedPeriod = requirePositive(Objects.requireNonNull(period,
                        this.scheduleMode + " 必须配置 period"), "period");
                if (dynamicDelay != null) {
                    throw new IllegalArgumentException(this.scheduleMode + " 不能配置 dynamicDelay");
                }
                resolvedDynamicDelay = null;
            }
            case DYNAMIC_DELAY -> {
                if (period != null) {
                    throw new IllegalArgumentException("DYNAMIC_DELAY 不能配置 period");
                }
                resolvedPeriod = null;
                resolvedDynamicDelay = Objects.requireNonNull(dynamicDelay,
                        "DYNAMIC_DELAY 必须配置 dynamicDelay");
            }
            default -> throw new AssertionError("unknown scheduleMode: " + this.scheduleMode);
        }
        this.period = resolvedPeriod;
        this.dynamicDelay = resolvedDynamicDelay;
    }

    private static Duration requireNonNegative(Duration duration, String field) {
        Objects.requireNonNull(duration, field + " 不能为空");
        if (duration.isNegative()) {
            throw new IllegalArgumentException(field + " 不能为负数，实际值=" + duration);
        }
        return duration;
    }

    private static Duration requirePositive(Duration duration, String field) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(field + " 必须为正数，实际值=" + duration);
        }
        return duration;
    }
}
