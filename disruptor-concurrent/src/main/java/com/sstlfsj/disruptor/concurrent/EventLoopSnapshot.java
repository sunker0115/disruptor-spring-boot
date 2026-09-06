package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import com.sstlfsj.disruptor.core.WorkerSnapshot;
import lombok.Builder;

import java.util.Objects;
import java.util.OptionalLong;

/** EventLoop 在某一时刻的不可变事实快照。 */
@Builder(toBuilder = true)
public record EventLoopSnapshot(
        String name,
        SupervisedLifecycle lifecycle,
        boolean acceptingTasks,
        CapacityMode capacityMode,
        OptionalLong capacityLimit,
        long outstandingTasks,
        long ingressPendingTasks,
        long scheduledPendingTasks,
        long executingTasks,
        long completedTasks,
        long failedTasks,
        long cancelledTasks,
        long shutdownNowReturnedTasks,
        long discardedTasks,
        int allocatedQueueSegments,
        int activeQueueSegments,
        Throwable failure,
        ShutdownMode shutdownMode,
        WorkerSnapshot worker) {

    public EventLoopSnapshot {
        Objects.requireNonNull(name, "name 不能为空");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        Objects.requireNonNull(lifecycle, "lifecycle 不能为空");
        Objects.requireNonNull(capacityMode, "capacityMode 不能为空");
        Objects.requireNonNull(capacityLimit, "capacityLimit 不能为空");
        Objects.requireNonNull(worker, "worker 不能为空");
        if (capacityMode == CapacityMode.BOUNDED
                && (capacityLimit.isEmpty() || capacityLimit.getAsLong() <= 0)) {
            throw new IllegalArgumentException("BOUNDED 快照必须包含正数 capacityLimit");
        }
        if (capacityMode == CapacityMode.UNBOUNDED && capacityLimit.isPresent()) {
            throw new IllegalArgumentException("UNBOUNDED 快照不能包含 capacityLimit");
        }
        requireNonNegative(outstandingTasks, "outstandingTasks");
        requireNonNegative(ingressPendingTasks, "ingressPendingTasks");
        requireNonNegative(scheduledPendingTasks, "scheduledPendingTasks");
        requireNonNegative(executingTasks, "executingTasks");
        requireNonNegative(completedTasks, "completedTasks");
        requireNonNegative(failedTasks, "failedTasks");
        requireNonNegative(cancelledTasks, "cancelledTasks");
        requireNonNegative(shutdownNowReturnedTasks, "shutdownNowReturnedTasks");
        requireNonNegative(discardedTasks, "discardedTasks");
        if (allocatedQueueSegments < 0) {
            throw new IllegalArgumentException("allocatedQueueSegments 不能为负数");
        }
        if (activeQueueSegments < 0) {
            throw new IllegalArgumentException("activeQueueSegments 不能为负数");
        }
        if (capacityMode == CapacityMode.BOUNDED
                && (allocatedQueueSegments != 0 || activeQueueSegments != 0)) {
            throw new IllegalArgumentException("BOUNDED 快照不报告无界队列 segment");
        }
        if (capacityMode == CapacityMode.UNBOUNDED
                && (allocatedQueueSegments == 0 || activeQueueSegments == 0)) {
            throw new IllegalArgumentException("UNBOUNDED 快照必须报告至少一个 segment");
        }
        if (activeQueueSegments > allocatedQueueSegments) {
            throw new IllegalArgumentException(
                    "activeQueueSegments 不能超过 allocatedQueueSegments");
        }
        if (capacityLimit.isPresent() && outstandingTasks > capacityLimit.getAsLong()) {
            throw new IllegalArgumentException("outstandingTasks 不能超过 capacityLimit");
        }
        if (acceptingTasks && lifecycle != SupervisedLifecycle.RUNNING) {
            throw new IllegalArgumentException("只有 RUNNING 生命周期可以接收任务");
        }
        if (worker.lifecycle() != lifecycle) {
            throw new IllegalArgumentException("EventLoop 与 worker 生命周期必须一致");
        }
    }

    public OptionalLong remainingCapacity() {
        return capacityLimit.isEmpty()
                ? OptionalLong.empty()
                : OptionalLong.of(capacityLimit.getAsLong() - outstandingTasks);
    }

    private static void requireNonNegative(long value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " 不能为负数");
        }
    }
}
