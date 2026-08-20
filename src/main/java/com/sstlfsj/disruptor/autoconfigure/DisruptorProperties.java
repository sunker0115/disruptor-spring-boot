package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.core.DisruptorConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties bound from the {@code disruptor.*} namespace，转换为 core 层无 Spring 的
 * {@link DisruptorConfig} 传给管道构建与生命周期。全局参数应用于所有管道；处理阶段拓扑由
 * {@code @DisruptorStage} 或编程式 {@code EventPipeline} 声明，不在此配置。
 */
@ConfigurationProperties(prefix = "disruptor")
public class DisruptorProperties {

    /** Ring buffer size per pipeline; must be a power of two. */
    private int bufferSize = 1024;

    /** Wait strategy used by consumers when no event is available. */
    private DisruptorConfig.WaitStrategyType waitStrategy = DisruptorConfig.WaitStrategyType.YIELDING;

    /** Maximum time to wait for each pipeline's ring buffer to drain during shutdown before halting. */
    private Duration shutdownTimeout = Duration.ofSeconds(10);

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public DisruptorConfig.WaitStrategyType getWaitStrategy() {
        return waitStrategy;
    }

    public void setWaitStrategy(DisruptorConfig.WaitStrategyType waitStrategy) {
        this.waitStrategy = waitStrategy;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    /** 转为 core 层的无 Spring 配置值对象。 */
    public DisruptorConfig toConfig() {
        return new DisruptorConfig(bufferSize, waitStrategy, shutdownTimeout);
    }
}
