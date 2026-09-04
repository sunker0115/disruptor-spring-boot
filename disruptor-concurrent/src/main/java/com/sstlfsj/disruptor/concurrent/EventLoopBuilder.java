package com.sstlfsj.disruptor.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;

/** 构建显式启动的有界或无界 EventLoop。 */
public final class EventLoopBuilder {

    private static final Logger log = LoggerFactory.getLogger(EventLoopBuilder.class);
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_COMMAND_BATCH_SIZE = 64;
    private static final int DEFAULT_TIMER_BATCH_SIZE = 64;

    private final String name;
    private final int capacity;
    private final List<EventLoopModule> modules = new ArrayList<>();

    private ThreadFactory threadFactory;
    private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
    private NanoClock clock = NanoClock.system();
    private int maxCommandBatchSize = DEFAULT_COMMAND_BATCH_SIZE;
    private int maxTimerBatchSize = DEFAULT_TIMER_BATCH_SIZE;
    private TaskExceptionHandler taskExceptionHandler = EventLoopBuilder::logTaskFailure;

    private EventLoopBuilder(String name, int capacity) {
        this.name = requireName(name);
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity 必须为 2 的幂，实际值=" + capacity);
        }
        this.capacity = capacity;
        this.threadFactory = command -> Thread.ofPlatform()
                .name(name + "-worker")
                .unstarted(command);
    }

    public static EventLoopBuilder bounded(String name, int capacity) {
        return new EventLoopBuilder(name, capacity);
    }

    public EventLoopBuilder threadFactory(ThreadFactory threadFactory) {
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory 不能为空");
        return this;
    }

    public EventLoopBuilder shutdownTimeout(Duration shutdownTimeout) {
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout 不能为空");
        if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout 必须为正数，实际值=" + shutdownTimeout);
        }
        shutdownTimeout.toNanos();
        this.shutdownTimeout = shutdownTimeout;
        return this;
    }

    public EventLoopBuilder clock(NanoClock clock) {
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        return this;
    }

    public EventLoopBuilder maxCommandBatchSize(int maxCommandBatchSize) {
        this.maxCommandBatchSize = requirePositive(maxCommandBatchSize, "maxCommandBatchSize");
        return this;
    }

    public EventLoopBuilder maxTimerBatchSize(int maxTimerBatchSize) {
        this.maxTimerBatchSize = requirePositive(maxTimerBatchSize, "maxTimerBatchSize");
        return this;
    }

    public EventLoopBuilder taskExceptionHandler(TaskExceptionHandler taskExceptionHandler) {
        this.taskExceptionHandler = Objects.requireNonNull(
                taskExceptionHandler, "taskExceptionHandler 不能为空");
        return this;
    }

    public EventLoopBuilder module(EventLoopModule module) {
        modules.add(Objects.requireNonNull(module, "module 不能为空"));
        return this;
    }

    public DisruptorEventLoop build() {
        return new DisruptorEventLoop(
                name,
                capacity,
                threadFactory,
                shutdownTimeout,
                clock,
                maxCommandBatchSize,
                maxTimerBatchSize,
                taskExceptionHandler,
                List.copyOf(modules));
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
}
