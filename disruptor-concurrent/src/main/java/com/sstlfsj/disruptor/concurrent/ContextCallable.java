package com.sstlfsj.disruptor.concurrent;

/** 显式接收不可变任务上下文的可调用任务。 */
@FunctionalInterface
public interface ContextCallable<V> {

    V call(TaskContext context) throws Exception;
}
