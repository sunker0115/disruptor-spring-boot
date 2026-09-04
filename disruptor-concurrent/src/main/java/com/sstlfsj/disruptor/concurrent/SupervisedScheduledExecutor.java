package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

/** 具有显式启动、受监督关闭和事实快照的调度执行器。 */
public interface SupervisedScheduledExecutor<S>
        extends ScheduledExecutorService, AutoCloseable {

    String name();

    CompletionStage<Void> start();

    void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline);

    CompletionStage<S> termination();

    S snapshot();
}
