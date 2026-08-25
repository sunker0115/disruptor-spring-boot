package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * 一条已构建管道的原生运行时入口。生命周期默认由 {@link DisruptorRuntime} 托管。
 *
 * @param <E> 事件类型
 */
public interface PipelineHandle<E> {

    String name();

    Class<E> eventType();

    Disruptor<E> disruptor();

    RingBuffer<E> ringBuffer();

    boolean isStarted();
}
