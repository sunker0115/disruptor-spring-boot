package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.function.Consumer;

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

    /**
     * 便捷阻塞发布：领取下一个槽位、用 {@code filler} 原地填充预分配事件后发布；
     * RingBuffer 满时阻塞直到有空位。
     *
     * <p>注意：{@code filler} 会被包装为一次性 {@code EventTranslator} lambda，每次调用产生一次
     * 该 lambda 的分配。追求零分配的极致低延迟路径请改用 {@link #ringBuffer()} + 静态
     * {@code EventTranslator}。
     */
    default void publish(Consumer<E> filler) {
        ringBuffer().publishEvent((event, sequence) -> filler.accept(event));
    }

    /**
     * 便捷非阻塞发布（背压入口）：RingBuffer 有空位时填充并发布返回 {@code true}；
     * 满时立即返回 {@code false}，不阻塞。分配权衡同 {@link #publish(Consumer)}。
     */
    default boolean tryPublish(Consumer<E> filler) {
        return ringBuffer().tryPublishEvent((event, sequence) -> filler.accept(event));
    }

    /** 剩余可写槽位数；{@code bufferSize - remaining()} 为近似积压量。 */
    default long remaining() {
        return ringBuffer().remainingCapacity();
    }
}
