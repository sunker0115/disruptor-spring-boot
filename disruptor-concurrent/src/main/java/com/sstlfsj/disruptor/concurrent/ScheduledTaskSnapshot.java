package com.sstlfsj.disruptor.concurrent;

import lombok.Builder;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** 高级调度任务在某一时刻的不可变事实快照。 */
@Builder(toBuilder = true)
public record ScheduledTaskSnapshot(
        long acceptedSequence,
        ScheduleMode scheduleMode,
        long triggerNanos,
        OptionalLong expiresAtNanos,
        int priority,
        int executions,
        OptionalInt maxExecutions,
        boolean started,
        TaskOutcome outcome,
        CancellationReason cancellationReason,
        Throwable lastFailure) {

    public ScheduledTaskSnapshot {
        if (acceptedSequence < 0) {
            throw new IllegalArgumentException("acceptedSequence 不能为负数");
        }
        Objects.requireNonNull(scheduleMode, "scheduleMode 不能为空");
        Objects.requireNonNull(expiresAtNanos, "expiresAtNanos 不能为空");
        Objects.requireNonNull(maxExecutions, "maxExecutions 不能为空");
        Objects.requireNonNull(outcome, "outcome 不能为空");
        if (executions < 0) {
            throw new IllegalArgumentException("executions 不能为负数");
        }
        if (maxExecutions.isPresent() && maxExecutions.getAsInt() <= 0) {
            throw new IllegalArgumentException("maxExecutions 必须为正数");
        }
        if (executions > 0 && !started) {
            throw new IllegalArgumentException("已执行的任务必须已开始");
        }
        if (outcome == TaskOutcome.CANCELLED && cancellationReason == null) {
            throw new IllegalArgumentException("CANCELLED 必须包含 cancellationReason");
        }
        if (outcome != TaskOutcome.CANCELLED && cancellationReason != null) {
            throw new IllegalArgumentException("非 CANCELLED 不能包含 cancellationReason");
        }
        if (outcome == TaskOutcome.FAILED && lastFailure == null) {
            throw new IllegalArgumentException("FAILED 必须包含 lastFailure");
        }
    }
}
