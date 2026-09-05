package com.sstlfsj.disruptor.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** 显式 EventLoop 根对象的 Spring 生命周期设置。 */
@ConfigurationProperties(prefix = "disruptor.concurrent")
public class DisruptorConcurrentProperties {

    /** 是否启用 EventLoop 根对象生命周期、健康和指标装配。 */
    private boolean enabled = true;

    /** Spring 生命周期阶段；值越小越早启动、越晚停止。 */
    private int lifecyclePhase = Integer.MIN_VALUE;

    /** 所有根对象共享的关闭时间边界。 */
    private Duration shutdownTimeout = Duration.ofSeconds(10);

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getLifecyclePhase() {
        return lifecyclePhase;
    }

    public void setLifecyclePhase(int lifecyclePhase) {
        this.lifecyclePhase = lifecyclePhase;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }
}
