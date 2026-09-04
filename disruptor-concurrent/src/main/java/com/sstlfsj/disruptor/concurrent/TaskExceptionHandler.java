package com.sstlfsj.disruptor.concurrent;

/** 处理用户任务抛出但不应终止基础设施的异常。 */
@FunctionalInterface
public interface TaskExceptionHandler {

    void handle(EventLoop loop, Runnable command, Throwable failure);
}
