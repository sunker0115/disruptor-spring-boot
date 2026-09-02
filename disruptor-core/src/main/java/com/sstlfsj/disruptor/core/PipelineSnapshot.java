package com.sstlfsj.disruptor.core;

import lombok.Builder;

import java.util.Objects;

/**
 * 管道在某一时刻的不可变状态快照。
 */
public record PipelineSnapshot(
        String name,
        PipelineLifecycle lifecycle,
        PipelineHealth health,
        boolean acceptingPublications,
        int expectedConsumers,
        int createdConsumers,
        int aliveConsumers,
        int bufferSize,
        long backlog,
        Throwable failure,
        boolean gracefulTermination) {

    public PipelineSnapshot {
        name = PipelineSettings.requireName(name);
        Objects.requireNonNull(lifecycle, "lifecycle 不能为空");
        Objects.requireNonNull(health, "health 不能为空");
        validateCounts(expectedConsumers, createdConsumers, aliveConsumers);
        validateNonNegative(bufferSize, "bufferSize");
        if (backlog < 0) {
            throw new IllegalArgumentException("backlog 不能为负数，实际值=" + backlog);
        }
        if (acceptingPublications && lifecycle != PipelineLifecycle.RUNNING) {
            throw new IllegalArgumentException("只有 RUNNING 状态的管道可以接收发布");
        }
        if (gracefulTermination && lifecycle != PipelineLifecycle.TERMINATED) {
            throw new IllegalArgumentException("只有 TERMINATED 状态的管道可以标记为优雅终止");
        }
        if (health != deriveHealth(lifecycle, failure, expectedConsumers, createdConsumers, aliveConsumers)) {
            throw new IllegalArgumentException("health 必须与管道状态一致");
        }
    }

    @Builder(builderMethodName = "builder")
    private static PipelineSnapshot buildSnapshot(
            String name,
            PipelineLifecycle lifecycle,
            boolean acceptingPublications,
            int expectedConsumers,
            int createdConsumers,
            int aliveConsumers,
            int bufferSize,
            long backlog,
            Throwable failure,
            boolean gracefulTermination) {
        PipelineHealth health = deriveHealth(
                lifecycle, failure, expectedConsumers, createdConsumers, aliveConsumers);
        return new PipelineSnapshot(name, lifecycle, health, acceptingPublications, expectedConsumers,
                createdConsumers, aliveConsumers, bufferSize, backlog, failure, gracefulTermination);
    }

    private static PipelineHealth deriveHealth(
            PipelineLifecycle lifecycle,
            Throwable failure,
            int expectedConsumers,
            int createdConsumers,
            int aliveConsumers) {
        Objects.requireNonNull(lifecycle, "lifecycle 不能为空");
        return switch (lifecycle) {
            case NEW, STARTING -> PipelineHealth.STARTING;
            case RUNNING -> failure == null
                    && expectedConsumers > 0
                    && createdConsumers == expectedConsumers
                    && aliveConsumers == expectedConsumers
                    ? PipelineHealth.HEALTHY
                    : PipelineHealth.UNHEALTHY;
            case QUIESCING, STOPPING -> PipelineHealth.OUT_OF_SERVICE;
            case TERMINATED -> PipelineHealth.TERMINATED;
        };
    }

    private static void validateCounts(int expectedConsumers, int createdConsumers, int aliveConsumers) {
        validateNonNegative(expectedConsumers, "expectedConsumers");
        validateNonNegative(createdConsumers, "createdConsumers");
        validateNonNegative(aliveConsumers, "aliveConsumers");
        if (createdConsumers > expectedConsumers) {
            throw new IllegalArgumentException("createdConsumers 不能大于 expectedConsumers");
        }
        if (aliveConsumers > createdConsumers) {
            throw new IllegalArgumentException("aliveConsumers 不能大于 createdConsumers");
        }
    }

    private static void validateNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " 不能为负数，实际值=" + value);
        }
    }
}
