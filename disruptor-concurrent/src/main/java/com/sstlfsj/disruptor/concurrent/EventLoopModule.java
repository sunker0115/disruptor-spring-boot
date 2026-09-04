package com.sstlfsj.disruptor.concurrent;

/** 与 EventLoop worker 同线程执行的轻量生命周期模块。 */
public interface EventLoopModule {

    default void onStart(EventLoop loop) throws Exception {
    }

    default void onUpdate(EventLoop loop, long nowNanos) throws Exception {
    }

    default void onStop(EventLoop loop) throws Exception {
    }
}
