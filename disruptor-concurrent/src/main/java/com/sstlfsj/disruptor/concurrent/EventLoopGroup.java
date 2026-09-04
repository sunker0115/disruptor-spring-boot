package com.sstlfsj.disruptor.concurrent;

/** 拥有固定子 EventLoop 集合的受监督调度执行器。 */
public interface EventLoopGroup
        extends SupervisedScheduledExecutor<EventLoopGroupSnapshot>, Iterable<EventLoop> {

    EventLoop next();

    EventLoop select(int affinityKey);
}
