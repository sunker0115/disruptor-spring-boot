package com.sstlfsj.disruptor.concurrent;

/** 为 Group 创建拥有稳定索引的子 EventLoop。 */
@FunctionalInterface
public interface EventLoopFactory {

    EventLoop create(EventLoopGroup parent, int childIndex);
}
