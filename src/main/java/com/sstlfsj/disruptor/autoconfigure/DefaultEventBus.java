package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.event.EventBus;
import com.sstlfsj.disruptor.event.EventPublisher;

import java.util.function.Consumer;

/**
 * {@link EventBus} 默认实现：按事件类型从 {@link Pipelines} 查对应管道并委托发布。
 */
public class DefaultEventBus implements EventBus {

    private final Pipelines pipelines;

    public DefaultEventBus(Pipelines pipelines) {
        this.pipelines = pipelines;
    }

    @Override
    public <E> EventPublisher<E> publisher(Class<E> eventType) {
        DisruptorPipeline<E> pipeline = pipelines.get(eventType);
        if (pipeline == null) {
            throw new IllegalArgumentException(
                    "没有为事件类型 " + eventType.getName() + " 声明的管道；"
                            + "需用 @DisruptorStage(pipeline=...) 声明处理阶段，且阶段方法参数为该类型");
        }
        return pipeline;
    }

    @Override
    public <E> void publish(Class<E> eventType, Consumer<E> filler) {
        publisher(eventType).publish(filler);
    }

    @Override
    public <E> boolean tryPublish(Class<E> eventType, Consumer<E> filler) {
        return publisher(eventType).tryPublish(filler);
    }

    @Override
    public long remainingCapacity(Class<?> eventType) {
        DisruptorPipeline<?> pipeline = pipelines.get(eventType);
        if (pipeline == null) {
            throw new IllegalArgumentException("没有为事件类型 " + eventType.getName() + " 声明的管道");
        }
        return pipeline.remainingCapacity();
    }
}
