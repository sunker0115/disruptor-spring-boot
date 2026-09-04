package com.sstlfsj.disruptor.concurrent;

/** 可与取消触发线性化竞争的监听器注册句柄。 */
public interface CancellationRegistration extends AutoCloseable {

    boolean unregister();

    @Override
    default void close() {
        unregister();
    }
}
