package com.sstlfsj.disruptor.core;

/** Runtime 未能在关闭预算内无损停止全部管道。 */
public final class DisruptorShutdownException extends RuntimeException {

    public DisruptorShutdownException(String message) {
        super(message);
    }
}
