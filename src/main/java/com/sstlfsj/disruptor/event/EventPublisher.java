package com.sstlfsj.disruptor.event;

import java.util.function.Consumer;

/**
 * 单条强类型管道的发布入口。事件对象在 ring buffer 中<strong>预分配</strong>，发布时通过
 * {@code filler} 原地填充字段（零分配），而非传入新对象——这是发挥 Disruptor 零分配与
 * 缓存局部性的关键。
 *
 * @param <E> 该管道的强类型事件类型
 */
public interface EventPublisher<E> {

    /**
     * 填充预分配事件后发布。ring buffer 满时<strong>阻塞</strong>发布线程直到有空槽。
     *
     * @param filler 对取自 ring buffer 槽位的（可能残留上轮数据的）事件对象原地赋值
     */
    void publish(Consumer<E> filler);

    /**
     * 非阻塞发布：ring buffer 有空槽时填充并返回 {@code true}；已满时立即返回 {@code false}，
     * 不阻塞发布线程（背压出口，供调用方降级）。
     */
    boolean tryPublish(Consumer<E> filler);

    /**
     * @return ring buffer 当前剩余可写槽位数（堆积 = bufferSize - 本值），供接入监控。
     */
    long remainingCapacity();
}
