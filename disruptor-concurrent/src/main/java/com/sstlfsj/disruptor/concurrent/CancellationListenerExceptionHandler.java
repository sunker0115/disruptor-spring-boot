package com.sstlfsj.disruptor.concurrent;

/** 取消监听器执行或异步投递失败的隔离边界。 */
@FunctionalInterface
public interface CancellationListenerExceptionHandler {

    void handle(CancellationReason reason, Throwable failure);
}
