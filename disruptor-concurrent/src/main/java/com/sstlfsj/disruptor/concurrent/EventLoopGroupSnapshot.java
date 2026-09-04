package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import lombok.Builder;

import java.util.List;
import java.util.Objects;

/** 固定 EventLoopGroup 在某一时刻的不可变聚合事实快照。 */
@Builder(toBuilder = true)
public record EventLoopGroupSnapshot(
        String name,
        SupervisedLifecycle lifecycle,
        boolean acceptingTasks,
        int childCount,
        long outstandingTasks,
        long executingTasks,
        long completedTasks,
        long failedTasks,
        long cancelledTasks,
        long shutdownNowReturnedTasks,
        Throwable failure,
        ShutdownMode shutdownMode,
        List<EventLoopSnapshot> children) {

    public EventLoopGroupSnapshot {
        Objects.requireNonNull(name, "name 不能为空");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        Objects.requireNonNull(lifecycle, "lifecycle 不能为空");
        Objects.requireNonNull(children, "children 不能为空");
        children = List.copyOf(children);
        if (childCount <= 0 || childCount != children.size()) {
            throw new IllegalArgumentException("childCount 必须为正数且与 children 数量一致");
        }
        requireNonNegative(outstandingTasks, "outstandingTasks");
        requireNonNegative(executingTasks, "executingTasks");
        requireNonNegative(completedTasks, "completedTasks");
        requireNonNegative(failedTasks, "failedTasks");
        requireNonNegative(cancelledTasks, "cancelledTasks");
        requireNonNegative(shutdownNowReturnedTasks, "shutdownNowReturnedTasks");
        if (acceptingTasks && lifecycle != SupervisedLifecycle.RUNNING) {
            throw new IllegalArgumentException("只有 RUNNING 生命周期可以接收任务");
        }
    }

    private static void requireNonNegative(long value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " 不能为负数");
        }
    }
}
