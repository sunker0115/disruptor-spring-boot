package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

/**
 * 一条管道的强类型定义。拓扑直接使用 LMAX {@code Disruptor<E>}，不映射或裁剪原生能力。
 *
 * @param <E> 事件类型
 */
public final class PipelineSpec<E> {

    private final String name;
    private final Class<E> eventType;
    private final EventFactory<E> eventFactory;
    private final DisruptorTopology<E> topology;
    private final Integer bufferSize;
    private final ProducerType producerType;
    private final Supplier<? extends WaitStrategy> waitStrategyFactory;
    private final ThreadFactory threadFactory;
    private final ExceptionHandler<? super E> exceptionHandler;

    private PipelineSpec(Builder<E> builder) {
        this.name = builder.name;
        this.eventType = builder.eventType;
        this.eventFactory = builder.eventFactory;
        if (builder.topology == null) {
            throw new IllegalStateException("topology 不能为空");
        }
        this.topology = builder.topology;
        this.bufferSize = builder.bufferSize;
        this.producerType = builder.producerType;
        this.waitStrategyFactory = builder.waitStrategyFactory;
        this.threadFactory = builder.threadFactory;
        this.exceptionHandler = builder.exceptionHandler;
    }

    public static <E> Builder<E> builder(String name, Class<E> eventType, EventFactory<E> eventFactory) {
        return new Builder<>(name, eventType, eventFactory);
    }

    public String name() {
        return name;
    }

    public Class<E> eventType() {
        return eventType;
    }

    EventFactory<E> eventFactory() {
        return eventFactory;
    }

    DisruptorTopology<E> topology() {
        return topology;
    }

    ExceptionHandler<? super E> exceptionHandler() {
        return exceptionHandler;
    }

    ResolvedPipelineSettings<E> resolve(PipelineSettings settings) {
        Objects.requireNonNull(settings, "settings 不能为空");
        int resolvedBufferSize = bufferSize == null ? settings.bufferSize() : bufferSize;
        ProducerType resolvedProducerType = producerType == null ? settings.producerType() : producerType;
        Supplier<? extends WaitStrategy> resolvedWaitStrategyFactory = waitStrategyFactory == null
                ? settings.waitStrategyFactory() : waitStrategyFactory;
        ThreadFactory resolvedThreadFactory = threadFactory == null
                ? settings.threadFactoryFactory().apply(name) : threadFactory;
        ExceptionHandler<? super E> resolvedExceptionHandler = exceptionHandler == null
                ? settings.exceptionHandler() : exceptionHandler;

        PipelineSettings.validateBufferSize(resolvedBufferSize);
        WaitStrategy resolvedWaitStrategy = Objects.requireNonNull(resolvedWaitStrategyFactory.get(),
                "waitStrategyFactory 不能返回 null");
        return new ResolvedPipelineSettings<>(
                resolvedBufferSize,
                Objects.requireNonNull(resolvedProducerType, "producerType 不能为空"),
                resolvedWaitStrategy,
                Objects.requireNonNull(resolvedThreadFactory, "threadFactory 不能为空"),
                Objects.requireNonNull(resolvedExceptionHandler, "exceptionHandler 不能为空"));
    }

    public static final class Builder<E> {

        private final String name;
        private final Class<E> eventType;
        private final EventFactory<E> eventFactory;
        private DisruptorTopology<E> topology;
        private Integer bufferSize;
        private ProducerType producerType;
        private Supplier<? extends WaitStrategy> waitStrategyFactory;
        private ThreadFactory threadFactory;
        private ExceptionHandler<? super E> exceptionHandler;

        private Builder(String name, Class<E> eventType, EventFactory<E> eventFactory) {
            this.name = PipelineSettings.requireName(name);
            this.eventType = Objects.requireNonNull(eventType, "eventType 不能为空");
            this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory 不能为空");
        }

        public Builder<E> topology(DisruptorTopology<E> topology) {
            this.topology = Objects.requireNonNull(topology, "topology 不能为空");
            return this;
        }

        public Builder<E> bufferSize(int bufferSize) {
            PipelineSettings.validateBufferSize(bufferSize);
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder<E> producerType(ProducerType producerType) {
            this.producerType = Objects.requireNonNull(producerType, "producerType 不能为空");
            return this;
        }

        public Builder<E> waitStrategy(Supplier<? extends WaitStrategy> waitStrategyFactory) {
            this.waitStrategyFactory = Objects.requireNonNull(waitStrategyFactory,
                    "waitStrategyFactory 不能为空");
            return this;
        }

        public Builder<E> threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory 不能为空");
            return this;
        }

        public Builder<E> exceptionHandler(ExceptionHandler<? super E> exceptionHandler) {
            this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "exceptionHandler 不能为空");
            return this;
        }

        public PipelineSpec<E> build() {
            return new PipelineSpec<>(this);
        }
    }
}
