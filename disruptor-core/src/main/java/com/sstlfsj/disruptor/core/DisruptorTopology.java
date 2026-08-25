package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.dsl.Disruptor;

/**
 * 使用 LMAX 原生 DSL 定义一条管道的完整消费拓扑。
 *
 * @param <E> 事件类型
 */
@FunctionalInterface
public interface DisruptorTopology<E> {

    void configure(Disruptor<E> disruptor);
}
