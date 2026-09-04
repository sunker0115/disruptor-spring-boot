package com.sstlfsj.disruptor.concurrent.internal;

import java.util.Objects;

/** 单一 EventLoop worker 入口。 */
final class EventLoopWorker implements Runnable {

    private final EventLoopKernel kernel;

    EventLoopWorker(EventLoopKernel kernel) {
        this.kernel = Objects.requireNonNull(kernel, "kernel 不能为空");
    }

    @Override
    public void run() {
        kernel.runWorker();
    }
}
