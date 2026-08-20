package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.function.Consumer;

/**
 * 一条强类型管道的运行时载体：封装某事件类型的 Disruptor 与 RingBuffer，并实现零分配发布。
 * ring buffer 槽位中是预分配的强类型事件实例（{@code E::new}），发布时由 filler 原地 mutate。
 *
 * @param <E> 该管道的强类型事件类型
 */
public class DisruptorPipeline<E> implements EventPublisher<E> {

    private final String name;
    private final Class<E> eventType;
    private final Disruptor<Object> disruptor;
    private final RingBuffer<Object> ringBuffer;

    public DisruptorPipeline(String name, Class<E> eventType, Disruptor<Object> disruptor) {
        this.name = name;
        this.eventType = eventType;
        this.disruptor = disruptor;
        this.ringBuffer = disruptor.getRingBuffer();
    }

    /** @return 管道名（用于日志与诊断）。 */
    public String name() {
        return name;
    }

    public Class<E> eventType() {
        return eventType;
    }

    public Disruptor<Object> disruptor() {
        return disruptor;
    }

    @Override
    public void publish(Consumer<E> filler) {
        long sequence = ringBuffer.next();
        writeAndPublish(sequence, filler);
    }

    @Override
    public boolean tryPublish(Consumer<E> filler) {
        long sequence;
        try {
            sequence = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return false;
        }
        writeAndPublish(sequence, filler);
        return true;
    }

    @Override
    public long remainingCapacity() {
        return ringBuffer.remainingCapacity();
    }

    @SuppressWarnings("unchecked")
    private void writeAndPublish(long sequence, Consumer<E> filler) {
        try {
            filler.accept((E) ringBuffer.get(sequence));
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
