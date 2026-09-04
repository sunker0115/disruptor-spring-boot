package com.sstlfsj.disruptor.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

/** 构建显式启动的有界或无界 EventLoop。 */
public final class EventLoopBuilder<T extends EventLoop> {

    private static final Logger log = LoggerFactory.getLogger(EventLoopBuilder.class);
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_COMMAND_BATCH_SIZE = 64;
    private static final int DEFAULT_TIMER_BATCH_SIZE = 64;

    private final String name;
    private final int queueSize;
    private final Function<Configuration, T> factory;
    private final List<EventLoopModule> modules = new ArrayList<>();

    private ThreadFactory threadFactory;
    private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
    private NanoClock clock = NanoClock.system();
    private int maxCommandBatchSize = DEFAULT_COMMAND_BATCH_SIZE;
    private int maxTimerBatchSize = DEFAULT_TIMER_BATCH_SIZE;
    private TaskExceptionHandler taskExceptionHandler = EventLoopBuilder::logTaskFailure;

    private EventLoopBuilder(
            String name,
            int queueSize,
            Function<Configuration, T> factory) {
        this.name = requireName(name);
        if (queueSize <= 0 || Integer.bitCount(queueSize) != 1) {
            throw new IllegalArgumentException("queueSize 必须为 2 的幂，实际值=" + queueSize);
        }
        this.queueSize = queueSize;
        this.factory = Objects.requireNonNull(factory, "factory 不能为空");
        this.threadFactory = command -> Thread.ofPlatform()
                .name(name + "-worker")
                .unstarted(command);
    }

    public static EventLoopBuilder<DisruptorEventLoop> bounded(String name, int capacity) {
        return new EventLoopBuilder<>(name, capacity, DisruptorEventLoop::new);
    }

    public static EventLoopBuilder<UnboundedEventLoop> unbounded(String name, int segmentSize) {
        return new EventLoopBuilder<>(name, segmentSize, UnboundedEventLoop::new);
    }

    public EventLoopBuilder<T> threadFactory(ThreadFactory threadFactory) {
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory 不能为空");
        return this;
    }

    public EventLoopBuilder<T> shutdownTimeout(Duration shutdownTimeout) {
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout 不能为空");
        if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout 必须为正数，实际值=" + shutdownTimeout);
        }
        shutdownTimeout.toNanos();
        this.shutdownTimeout = shutdownTimeout;
        return this;
    }

    public EventLoopBuilder<T> clock(NanoClock clock) {
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        return this;
    }

    public EventLoopBuilder<T> maxCommandBatchSize(int maxCommandBatchSize) {
        this.maxCommandBatchSize = requirePositive(maxCommandBatchSize, "maxCommandBatchSize");
        return this;
    }

    public EventLoopBuilder<T> maxTimerBatchSize(int maxTimerBatchSize) {
        this.maxTimerBatchSize = requirePositive(maxTimerBatchSize, "maxTimerBatchSize");
        return this;
    }

    public EventLoopBuilder<T> taskExceptionHandler(TaskExceptionHandler taskExceptionHandler) {
        this.taskExceptionHandler = Objects.requireNonNull(
                taskExceptionHandler, "taskExceptionHandler 不能为空");
        return this;
    }

    public EventLoopBuilder<T> module(EventLoopModule module) {
        modules.add(Objects.requireNonNull(module, "module 不能为空"));
        return this;
    }

    public T build() {
        return factory.apply(new Configuration(
                name,
                queueSize,
                threadFactory,
                shutdownTimeout,
                clock,
                maxCommandBatchSize,
                maxTimerBatchSize,
                taskExceptionHandler,
                List.copyOf(modules)));
    }

    private static int requirePositive(int value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " 必须为正数，实际值=" + value);
        }
        return value;
    }

    private static String requireName(String value) {
        Objects.requireNonNull(value, "name 不能为空");
        if (value.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        return value;
    }

    private static void logTaskFailure(EventLoop loop, Runnable command, Throwable failure) {
        log.error("EventLoop 裸任务执行失败：loop={}，command={}", loop.name(), command, failure);
    }

    /** 门面构造期间一次性传给唯一 kernel 的不可变配置。 */
    record Configuration(
            String name,
            int queueSize,
            ThreadFactory threadFactory,
            Duration shutdownTimeout,
            NanoClock clock,
            int maxCommandBatchSize,
            int maxTimerBatchSize,
            TaskExceptionHandler taskExceptionHandler,
            List<EventLoopModule> modules) {
    }
}
