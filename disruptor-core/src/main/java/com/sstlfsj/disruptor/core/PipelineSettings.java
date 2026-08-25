package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 管道基础设施的完整默认设置。{@link PipelineSpec} 中显式声明的选项优先级更高。
 */
public record PipelineSettings(
        int bufferSize,
        ProducerType producerType,
        Supplier<? extends WaitStrategy> waitStrategyFactory,
        Function<String, ThreadFactory> threadFactoryFactory,
        Duration shutdownTimeout,
        ExceptionHandler<Object> exceptionHandler) {

    public PipelineSettings {
        validateBufferSize(bufferSize);
        Objects.requireNonNull(producerType, "producerType 不能为空");
        Objects.requireNonNull(waitStrategyFactory, "waitStrategyFactory 不能为空");
        Objects.requireNonNull(threadFactoryFactory, "threadFactoryFactory 不能为空");
        validateShutdownTimeout(shutdownTimeout);
        Objects.requireNonNull(exceptionHandler, "exceptionHandler 不能为空");
    }

    public static PipelineSettings defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static boolean isPowerOfTwo(int value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    static void validateBufferSize(int bufferSize) {
        if (!isPowerOfTwo(bufferSize)) {
            throw new IllegalArgumentException("bufferSize 必须是正的 2 的幂，实际值=" + bufferSize);
        }
    }

    static void validateShutdownTimeout(Duration shutdownTimeout) {
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout 不能为空");
        if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout 必须大于 0，实际值=" + shutdownTimeout);
        }
    }

    public static final class Builder {

        private int bufferSize = 1024;
        private ProducerType producerType = ProducerType.MULTI;
        private Supplier<? extends WaitStrategy> waitStrategyFactory = BlockingWaitStrategy::new;
        private Function<String, ThreadFactory> threadFactoryFactory = NamedThreadFactory::new;
        private Duration shutdownTimeout = Duration.ofSeconds(10);
        private ExceptionHandler<Object> exceptionHandler = ErrorStrategy.LOG_AND_CONTINUE.handler();

        private Builder() {
        }

        public Builder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder producerType(ProducerType producerType) {
            this.producerType = Objects.requireNonNull(producerType, "producerType 不能为空");
            return this;
        }

        public Builder waitStrategy(Supplier<? extends WaitStrategy> waitStrategyFactory) {
            this.waitStrategyFactory = Objects.requireNonNull(waitStrategyFactory,
                    "waitStrategyFactory 不能为空");
            return this;
        }

        public Builder threadFactory(Function<String, ThreadFactory> threadFactoryFactory) {
            this.threadFactoryFactory = Objects.requireNonNull(threadFactoryFactory,
                    "threadFactoryFactory 不能为空");
            return this;
        }

        public Builder shutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public Builder exceptionHandler(ExceptionHandler<Object> exceptionHandler) {
            this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "exceptionHandler 不能为空");
            return this;
        }

        public PipelineSettings build() {
            return new PipelineSettings(bufferSize, producerType, waitStrategyFactory,
                    threadFactoryFactory, shutdownTimeout, exceptionHandler);
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {

        private final String pipelineName;
        private final AtomicInteger sequence = new AtomicInteger(1);

        private NamedThreadFactory(String pipelineName) {
            this.pipelineName = requireName(pipelineName);
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable,
                    "disruptor-" + pipelineName + "-" + sequence.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }

    static String requireName(String name) {
        Objects.requireNonNull(name, "管道名不能为空");
        if (name.isBlank()) {
            throw new IllegalArgumentException("管道名不能为空白");
        }
        return name;
    }
}
