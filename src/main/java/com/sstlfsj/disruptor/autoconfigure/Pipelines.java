package com.sstlfsj.disruptor.autoconfigure;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 所有强类型管道的容器，按事件类型索引。由 {@link PipelineRegistrar} 在启动时填充，
 * {@link DefaultEventBus} 与 {@link DisruptorLifecycle} 读取。
 */
public class Pipelines {

    private final Map<Class<?>, DisruptorPipeline<?>> byType = new ConcurrentHashMap<>();

    public void register(DisruptorPipeline<?> pipeline) {
        byType.put(pipeline.eventType(), pipeline);
    }

    @SuppressWarnings("unchecked")
    public <E> DisruptorPipeline<E> get(Class<E> eventType) {
        return (DisruptorPipeline<E>) byType.get(eventType);
    }

    public Collection<DisruptorPipeline<?>> all() {
        return byType.values();
    }
}
