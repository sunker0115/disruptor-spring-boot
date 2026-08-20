package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties for the disruptor starter, bound from the {@code disruptor.*} namespace.
 * 全局参数应用于所有管道；处理阶段拓扑由 {@code @DisruptorStage} 注解声明，不在此配置。
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

    /** Ring buffer size per pipeline; must be a power of two. */
    private int bufferSize = 1024;

    /** Wait strategy used by consumers when no event is available. */
    private WaitStrategyType waitStrategy = WaitStrategyType.YIELDING;

    /** Maximum time to wait for each pipeline's ring buffer to drain during shutdown before halting. */
    private Duration shutdownTimeout = Duration.ofSeconds(10);

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

    /**
     * Maps the configured wait strategy to the corresponding LMAX Disruptor {@link WaitStrategy}.
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
