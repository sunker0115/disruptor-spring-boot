package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import java.time.Duration;

/**
 * 管道构建配置（无 Spring 依赖）：ring buffer 大小、等待策略、关闭超时。由上层（Spring Boot
 * 的 DisruptorProperties 或调用方手工）构造后传给 {@link PipelineBuilder} 与生命周期。
 */
public class DisruptorConfig {

    /** 等待策略类型，映射到 LMAX Disruptor 实现。 */
    public enum WaitStrategyType {
        BLOCKING,
        YIELDING,
        BUSY_SPIN,
        SLEEPING
    }

    private final int bufferSize;
    private final WaitStrategyType waitStrategy;
    private final Duration shutdownTimeout;

    public DisruptorConfig(int bufferSize, WaitStrategyType waitStrategy, Duration shutdownTimeout) {
        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;
        this.shutdownTimeout = shutdownTimeout;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public WaitStrategyType waitStrategyType() {
        return waitStrategy;
    }

    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    /** 映射为 LMAX Disruptor 的 {@link WaitStrategy} 实例。 */
    public WaitStrategy createWaitStrategy() {
        return switch (waitStrategy) {
            case BLOCKING -> new BlockingWaitStrategy();
            case BUSY_SPIN -> new BusySpinWaitStrategy();
            case SLEEPING -> new SleepingWaitStrategy();
            case YIELDING -> new YieldingWaitStrategy();
        };
    }
}
