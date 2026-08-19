package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration properties for the disruptor starter, bound from the
 * {@code disruptor.*} namespace.
 */
@ConfigurationProperties(prefix = "disruptor")
public class DisruptorProperties {

    /** Supported wait strategy types, mapped to LMAX Disruptor strategies. */
    public enum WaitStrategyType {
        BLOCKING,
        YIELDING,
        BUSY_SPIN,
        SLEEPING
    }

    /** Ring buffer size; must be a power of two. */
    private int bufferSize = 1024;

    /** Wait strategy used by consumers when no event is available. */
    private WaitStrategyType waitStrategy = WaitStrategyType.YIELDING;

    /**
     * Maximum time to wait for the ring buffer to drain during shutdown before
     * forcibly halting the disruptor. Bound from {@code disruptor.shutdown-timeout}.
     */
    private Duration shutdownTimeout = Duration.ofSeconds(10);

    /**
     * 处理阶段流水线：key 为阶段名，值声明该阶段依赖的上游阶段（after）。
     * 用于表达 Disruptor 消费者依赖图；{@code default} 阶段隐式存在、无需声明。
     * 未配置时仅有隐式 default 阶段，行为与无流水线一致。
     */
    private Map<String, StageDefinition> pipeline = new LinkedHashMap<>();

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public WaitStrategyType getWaitStrategy() {
        return waitStrategy;
    }

    public void setWaitStrategy(WaitStrategyType waitStrategy) {
        this.waitStrategy = waitStrategy;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public Map<String, StageDefinition> getPipeline() {
        return pipeline;
    }

    public void setPipeline(Map<String, StageDefinition> pipeline) {
        this.pipeline = pipeline;
    }

    /**
     * Maps the configured wait strategy to the corresponding LMAX Disruptor
     * {@link WaitStrategy} implementation.
     *
     * @return the Disruptor wait strategy instance
     */
    public WaitStrategy createWaitStrategy() {
        switch (waitStrategy) {
            case BLOCKING:
                return new BlockingWaitStrategy();
            case BUSY_SPIN:
                return new BusySpinWaitStrategy();
            case SLEEPING:
                return new SleepingWaitStrategy();
            case YIELDING:
                return new YieldingWaitStrategy();
            default:
                throw new IllegalStateException("Unsupported wait strategy: " + waitStrategy);
        }
    }
}
