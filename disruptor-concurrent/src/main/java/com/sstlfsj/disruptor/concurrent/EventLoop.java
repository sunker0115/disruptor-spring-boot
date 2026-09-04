package com.sstlfsj.disruptor.concurrent;

/** 严格单线程的任务执行与调度边界。 */
public interface EventLoop extends SupervisedScheduledExecutor<EventLoopSnapshot> {

    boolean inEventLoop();

    EventLoopGroup parent();

    boolean tryExecute(Runnable command);

    <V> EventLoopScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec);
}
