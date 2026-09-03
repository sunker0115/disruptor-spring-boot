package com.sstlfsj.disruptor.core;

/** 队列后端映射到统一受监督关闭状态机的非阻塞阶段协议。 */
public interface ShutdownBackend {

    void beginQuiesce() throws Throwable;

    boolean isDrained() throws Throwable;

    void stop(ShutdownMode mode) throws Throwable;
}
