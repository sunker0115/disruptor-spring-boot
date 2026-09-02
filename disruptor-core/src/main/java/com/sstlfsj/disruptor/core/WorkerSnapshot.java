package com.sstlfsj.disruptor.core;

import java.util.Objects;

/**
 * worker 监督器在某一时刻的不可变状态快照。
 *
 * @param failure 首个失败原因；尚未失败时为 {@code null}
 * @param shutdownMode 已请求的停机方式；尚未请求停机时为 {@code null}
 */
public record WorkerSnapshot(
        String name,
        PipelineLifecycle lifecycle,
        int expectedWorkers,
        int registeredWorkers,
        int aliveWorkers,
        Throwable failure,
        ShutdownMode shutdownMode,
        boolean gracefulTermination) {

    public WorkerSnapshot {
        Objects.requireNonNull(name, "name 不能为空");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        Objects.requireNonNull(lifecycle, "lifecycle 不能为空");
        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers 必须大于 0，实际值=" + expectedWorkers);
        }
        if (registeredWorkers < 0 || registeredWorkers > expectedWorkers) {
            throw new IllegalArgumentException(
                    "registeredWorkers 必须在 0 到 expectedWorkers 之间，实际值=" + registeredWorkers);
        }
        if (aliveWorkers < 0 || aliveWorkers > registeredWorkers) {
            throw new IllegalArgumentException(
                    "aliveWorkers 必须在 0 到 registeredWorkers 之间，实际值=" + aliveWorkers);
        }
        if (gracefulTermination
                && (lifecycle != PipelineLifecycle.TERMINATED
                || shutdownMode != ShutdownMode.GRACEFUL
                || failure != null
                || aliveWorkers != 0)) {
            throw new IllegalArgumentException("优雅终止标记必须与终止状态一致");
        }
    }
}
