package com.sstlfsj.disruptor.core;

import lombok.Builder;

import java.util.Objects;

/** worker 监督器在某一时刻的不可变事实快照。 */
@Builder(builderMethodName = "builder", toBuilder = true)
public record WorkerSnapshot(
        String name,
        PipelineLifecycle lifecycle,
        boolean registrationSealed,
        int expectedWorkers,
        int registeredWorkers,
        int startedWorkers,
        int aliveWorkers,
        Throwable failure,
        ShutdownMode shutdownMode,
        boolean reachedRunning,
        boolean drainCommitted,
        boolean gracefulStopApplied) {

    public WorkerSnapshot {
        Objects.requireNonNull(name, "name 不能为空");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        Objects.requireNonNull(lifecycle, "lifecycle 不能为空");
        validateCounts(registeredWorkers, startedWorkers, aliveWorkers);
        if (expectedWorkers < 0) {
            throw new IllegalArgumentException("expectedWorkers 不能为负数，实际值=" + expectedWorkers);
        }
        if (registrationSealed && expectedWorkers != registeredWorkers) {
            throw new IllegalArgumentException("封口后 expectedWorkers 必须等于 registeredWorkers");
        }
        if (!registrationSealed && expectedWorkers != 0) {
            throw new IllegalArgumentException("封口前 expectedWorkers 必须为 0");
        }
        validateLifecycle(
                lifecycle,
                registrationSealed,
                expectedWorkers,
                registeredWorkers,
                startedWorkers,
                aliveWorkers,
                failure,
                shutdownMode,
                reachedRunning,
                drainCommitted,
                gracefulStopApplied);
    }

    public boolean gracefulTermination() {
        return lifecycle == PipelineLifecycle.TERMINATED
                && registrationSealed
                && reachedRunning
                && drainCommitted
                && gracefulStopApplied
                && shutdownMode == ShutdownMode.GRACEFUL
                && failure == null
                && startedWorkers == registeredWorkers
                && aliveWorkers == 0;
    }

    private static void validateLifecycle(
            PipelineLifecycle lifecycle,
            boolean registrationSealed,
            int expectedWorkers,
            int registeredWorkers,
            int startedWorkers,
            int aliveWorkers,
            Throwable failure,
            ShutdownMode shutdownMode,
            boolean reachedRunning,
            boolean drainCommitted,
            boolean gracefulStopApplied) {
        if (lifecycle == PipelineLifecycle.RUNNING
                && (!registrationSealed
                || expectedWorkers == 0
                || startedWorkers != registeredWorkers
                || aliveWorkers != registeredWorkers
                || !reachedRunning
                || failure != null
                || shutdownMode != null)) {
            throw new IllegalArgumentException("RUNNING 必须对应已封口且全部入场存活的无故障 worker 集");
        }
        if ((lifecycle == PipelineLifecycle.NEW || lifecycle == PipelineLifecycle.STARTING)
                && (reachedRunning || shutdownMode != null)) {
            throw new IllegalArgumentException("启动阶段不能包含已运行或关闭事实");
        }
        if (lifecycle == PipelineLifecycle.QUIESCING
                && (!reachedRunning || shutdownMode != ShutdownMode.GRACEFUL || drainCommitted)) {
            throw new IllegalArgumentException("QUIESCING 必须是尚未提交排空的优雅关闭阶段");
        }
        if ((lifecycle == PipelineLifecycle.STOPPING || lifecycle == PipelineLifecycle.TERMINATED)
                && shutdownMode == null) {
            throw new IllegalArgumentException("停止阶段必须包含 shutdownMode");
        }
        if (lifecycle == PipelineLifecycle.TERMINATED && aliveWorkers != 0) {
            throw new IllegalArgumentException("TERMINATED 状态不能仍有存活 worker");
        }
        if (drainCommitted && !reachedRunning) {
            throw new IllegalArgumentException("提交排空前必须曾进入 RUNNING");
        }
        if (drainCommitted
                && lifecycle != PipelineLifecycle.STOPPING
                && lifecycle != PipelineLifecycle.TERMINATED) {
            throw new IllegalArgumentException("提交排空只能是停止阶段的历史事实");
        }
        if (gracefulStopApplied
                && lifecycle != PipelineLifecycle.STOPPING
                && lifecycle != PipelineLifecycle.TERMINATED) {
            throw new IllegalArgumentException("graceful stop 只能发生在停止阶段");
        }
        if (gracefulStopApplied && reachedRunning && !drainCommitted) {
            throw new IllegalArgumentException("运行后的 graceful stop 必须先提交排空");
        }
    }

    private static void validateCounts(int registeredWorkers, int startedWorkers, int aliveWorkers) {
        if (registeredWorkers < 0) {
            throw new IllegalArgumentException("registeredWorkers 不能为负数");
        }
        if (startedWorkers < 0 || startedWorkers > registeredWorkers) {
            throw new IllegalArgumentException("startedWorkers 必须在 0 到 registeredWorkers 之间");
        }
        if (aliveWorkers < 0 || aliveWorkers > startedWorkers) {
            throw new IllegalArgumentException("aliveWorkers 必须在 0 到 startedWorkers 之间");
        }
    }
}
