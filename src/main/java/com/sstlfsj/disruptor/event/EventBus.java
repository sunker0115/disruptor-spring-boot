package com.sstlfsj.disruptor.event;

import java.util.function.Consumer;

/**
 * 跨管道发布门面：按事件类型定位对应的强类型管道并发布。每种事件类型对应唯一一条
 * {@link EventPublisher} 管道（由 {@link DisruptorStage} 声明）。
 *
 * <p>对未声明管道的事件类型调用任一方法都会抛出 {@link IllegalArgumentException}。</p>
 */
public interface EventBus {

    /**
     * @return 指定事件类型的发布入口
     * @throws IllegalArgumentException 该事件类型没有声明对应管道
     */
    <E> EventPublisher<E> publisher(Class<E> eventType);

    /** 便捷方法，等价于 {@code publisher(eventType).publish(filler)}。 */
    <E> void publish(Class<E> eventType, Consumer<E> filler);

    /** 便捷方法，等价于 {@code publisher(eventType).tryPublish(filler)}。 */
    <E> boolean tryPublish(Class<E> eventType, Consumer<E> filler);

    /** @return 指定事件类型管道的剩余可写槽位数。 */
    long remainingCapacity(Class<?> eventType);
}
