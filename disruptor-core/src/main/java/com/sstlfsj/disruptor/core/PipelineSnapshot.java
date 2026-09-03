package com.sstlfsj.disruptor.core;

import lombok.Builder;

import java.util.Objects;

/** 管道在某一时刻的不可变事实快照。 */
@Builder(builderMethodName = "builder", toBuilder = true)
public record PipelineSnapshot(
        String name,
        PipelineLifecycle lifecycle,
        boolean acceptingPublications,
        boolean registrationSealed,
        int expectedConsumers,
        int createdConsumers,
        int startedConsumers,
        int aliveConsumers,
        int bufferSize,
        long backlog,
        Throwable failure,
        ShutdownMode shutdownMode,
        boolean reachedRunning,
        boolean drainCommitted,
        boolean gracefulStopApplied) {

    public PipelineSnapshot {
        name = PipelineSettings.requireName(name);
        Objects.requireNonNull(lifecycle, "lifecycle 不能为空");
        validateCounts(expectedConsumers, createdConsumers, startedConsumers, aliveConsumers,
                registrationSealed);
        if (bufferSize < 0) {
            throw new IllegalArgumentException("bufferSize 不能为负数，实际值=" + bufferSize);
        }
        if (backlog < 0) {
            throw new IllegalArgumentException("backlog 不能为负数，实际值=" + backlog);
        }
        validateLifecycle(
                lifecycle,
                acceptingPublications,
                registrationSealed,
                expectedConsumers,
                createdConsumers,
                startedConsumers,
                aliveConsumers,
                failure,
                shutdownMode,
                reachedRunning,
                drainCommitted,
                gracefulStopApplied);
    }

    public PipelineHealth health() {
        return switch (lifecycle) {
            case NEW, STARTING -> PipelineHealth.STARTING;
            case RUNNING -> failure == null
                    && acceptingPublications
                    && registrationSealed
                    && expectedConsumers > 0
                    && createdConsumers == expectedConsumers
                    && startedConsumers == expectedConsumers
                    && aliveConsumers == expectedConsumers
                    ? PipelineHealth.HEALTHY
                    : PipelineHealth.UNHEALTHY;
            case QUIESCING, STOPPING -> PipelineHealth.OUT_OF_SERVICE;
            case TERMINATED -> PipelineHealth.TERMINATED;
        };
    }

    public boolean gracefulTermination() {
        return lifecycle == PipelineLifecycle.TERMINATED
                && !acceptingPublications
                && registrationSealed
                && reachedRunning
                && drainCommitted
                && gracefulStopApplied
                && shutdownMode == ShutdownMode.GRACEFUL
                && failure == null
                && createdConsumers == expectedConsumers
                && startedConsumers == createdConsumers
                && aliveConsumers == 0;
    }

    private static void validateLifecycle(
            PipelineLifecycle lifecycle,
            boolean acceptingPublications,
            boolean registrationSealed,
            int expectedConsumers,
            int createdConsumers,
            int startedConsumers,
            int aliveConsumers,
            Throwable failure,
            ShutdownMode shutdownMode,
            boolean reachedRunning,
            boolean drainCommitted,
            boolean gracefulStopApplied) {
        if (acceptingPublications && lifecycle != PipelineLifecycle.RUNNING) {
            throw new IllegalArgumentException("只有 RUNNING 生命周期可以接收发布");
        }
        if (lifecycle == PipelineLifecycle.RUNNING
                && (!registrationSealed
                || expectedConsumers == 0
                || createdConsumers != expectedConsumers
                || !reachedRunning
                || shutdownMode != null)) {
            throw new IllegalArgumentException("RUNNING 必须对应已封口且曾成功启动的 consumer 集");
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
        if (lifecycle == PipelineLifecycle.TERMINATED
                && (!registrationSealed || aliveConsumers != 0)) {
            throw new IllegalArgumentException("TERMINATED 状态必须已封口且没有存活 consumer");
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

    private static void validateCounts(
            int expectedConsumers,
            int createdConsumers,
            int startedConsumers,
            int aliveConsumers,
            boolean registrationSealed) {
        if (expectedConsumers < 0 || createdConsumers < 0 || startedConsumers < 0 || aliveConsumers < 0) {
            throw new IllegalArgumentException("consumer 计数不能为负数");
        }
        if (startedConsumers > createdConsumers || aliveConsumers > startedConsumers) {
            throw new IllegalArgumentException("consumer 计数必须满足 alive <= started <= created");
        }
        if (registrationSealed && expectedConsumers != createdConsumers) {
            throw new IllegalArgumentException("封口后 expectedConsumers 必须等于 createdConsumers");
        }
        if (!registrationSealed && expectedConsumers != 0) {
            throw new IllegalArgumentException("封口前 expectedConsumers 必须为 0");
        }
    }
}
