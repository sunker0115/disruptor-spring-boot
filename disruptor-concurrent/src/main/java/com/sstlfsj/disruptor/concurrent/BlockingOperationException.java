package com.sstlfsj.disruptor.concurrent;

/** 在所属 EventLoop 线程上尝试等待未完成结果时抛出。 */
public final class BlockingOperationException extends IllegalStateException {

    public BlockingOperationException(String message) {
        super(message);
    }

    public BlockingOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
