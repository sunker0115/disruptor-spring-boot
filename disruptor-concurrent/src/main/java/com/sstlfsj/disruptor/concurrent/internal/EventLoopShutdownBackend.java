package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.core.ShutdownBackend;
import com.sstlfsj.disruptor.core.ShutdownMode;

import java.util.Objects;

/** 将 supervisor 的非阻塞关闭阶段映射为 worker 可见信号。 */
final class EventLoopShutdownBackend implements ShutdownBackend {

    private final EventLoopKernel kernel;

    EventLoopShutdownBackend(EventLoopKernel kernel) {
        this.kernel = Objects.requireNonNull(kernel, "kernel 不能为空");
    }

    @Override
    public void beginQuiesce() {
        kernel.beginQuiesce();
    }

    @Override
    public boolean isDrained() {
        return kernel.workerDrained();
    }

    @Override
    public void stop(ShutdownMode mode) {
        kernel.stopWorker(mode);
    }
}
