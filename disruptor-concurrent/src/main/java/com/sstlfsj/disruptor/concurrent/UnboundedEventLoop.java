package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.concurrent.internal.EventLoopKernel;

/** 显式选择的无界、严格单线程 EventLoop。 */
public final class UnboundedEventLoop extends AbstractEventLoop {

    UnboundedEventLoop(EventLoopBuilder.Configuration configuration) {
        initializeKernel(EventLoopKernel.unbounded(
                this,
                configuration.name(),
                configuration.queueSize(),
                configuration.threadFactory(),
                configuration.shutdownTimeout(),
                configuration.clock(),
                configuration.maxCommandBatchSize(),
                configuration.maxTimerBatchSize(),
                configuration.maxPooledSegments(),
                configuration.taskExceptionHandler(),
                configuration.modules()));
    }
}
