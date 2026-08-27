package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.core.ErrorStrategy;
import com.sstlfsj.disruptor.core.PipelineSettings;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Disruptor 自动配置属性。 */
@ConfigurationProperties(prefix = "disruptor")
public class DisruptorProperties {

    /** 是否启用自动配置。 */
    private boolean enabled = true;

    /** Spring 生命周期阶段；值越小越早启动、越晚停止。 */
    private int lifecyclePhase = Integer.MIN_VALUE;

    /** 所有管道的默认设置。 */
    private Defaults defaults = new Defaults();

    /** 按管道名覆盖默认设置。 */
    private Map<String, Pipeline> pipelines = new LinkedHashMap<>();

    /** Micrometer 只读指标设置。 */
    private Metrics metrics = new Metrics();

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

    public Defaults getDefaults() {
        return defaults;
    }

    public void setDefaults(Defaults defaults) {
        this.defaults = defaults;
    }

    public Map<String, Pipeline> getPipelines() {
        return pipelines;
    }

    public void setPipelines(Map<String, Pipeline> pipelines) {
        this.pipelines = pipelines;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    PipelineSettings settingsFor(String pipelineName) {
        Pipeline override = pipelines.get(pipelineName);
        int bufferSize = override == null || override.bufferSize == null
                ? defaults.bufferSize : override.bufferSize;
        ProducerType producerType = override == null || override.producerType == null
                ? defaults.producerType : override.producerType;
        WaitStrategyType waitStrategy = override == null || override.waitStrategy == null
                ? defaults.waitStrategy : override.waitStrategy;
        Duration shutdownTimeout = override == null || override.shutdownTimeout == null
                ? defaults.shutdownTimeout : override.shutdownTimeout;
        boolean daemonThreads = override == null || override.daemonThreads == null
                ? defaults.daemonThreads : override.daemonThreads;
        ErrorStrategy errorStrategy = override == null || override.errorStrategy == null
                ? defaults.errorStrategy : override.errorStrategy;

        try {
            return PipelineSettings.builder()
                    .bufferSize(bufferSize)
                    .producerType(producerType)
                    .waitStrategy(waitStrategy.factory())
                    .threadFactory(name -> new NamedThreadFactory(name, daemonThreads))
                    .shutdownTimeout(shutdownTimeout)
                    .exceptionHandler(errorStrategy.handler())
                    .build();
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "管道 '" + pipelineName + "' 配置无效：" + failure.getMessage(), failure);
        }
    }

    public static class Defaults {

        /** RingBuffer 容量，必须是正的 2 的幂。 */
        private int bufferSize = 1024;

        /** 生产者类型。 */
        private ProducerType producerType = ProducerType.MULTI;

        /** 常用等待策略预设；需要构造参数的策略应在 PipelineSpec 中直接配置。 */
        private WaitStrategyType waitStrategy = WaitStrategyType.BLOCKING;

        /** 单条管道关闭时等待积压事件排空的最长时间。 */
        private Duration shutdownTimeout = Duration.ofSeconds(10);

        /** 消费线程是否为 daemon 线程。 */
        private boolean daemonThreads;

        /** 消费异常的默认处置策略；默认停止失败消费者，避免失败槽位继续流向下游。 */
        private ErrorStrategy errorStrategy = ErrorStrategy.HALT;

        public int getBufferSize() {
            return bufferSize;
        }

        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        public ProducerType getProducerType() {
            return producerType;
        }

        public void setProducerType(ProducerType producerType) {
            this.producerType = producerType;
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

        public boolean isDaemonThreads() {
            return daemonThreads;
        }

        public void setDaemonThreads(boolean daemonThreads) {
            this.daemonThreads = daemonThreads;
        }

        public ErrorStrategy getErrorStrategy() {
            return errorStrategy;
        }

        public void setErrorStrategy(ErrorStrategy errorStrategy) {
            this.errorStrategy = errorStrategy;
        }
    }

    public static class Pipeline {

        /** 覆盖此管道的 RingBuffer 容量。 */
        private Integer bufferSize;

        /** 覆盖此管道的生产者类型。 */
        private ProducerType producerType;

        /** 覆盖此管道的等待策略预设。 */
        private WaitStrategyType waitStrategy;

        /** 覆盖此管道的关闭等待时间。 */
        private Duration shutdownTimeout;

        /** 覆盖此管道的 daemon 线程设置。 */
        private Boolean daemonThreads;

        /** 覆盖此管道的消费异常处置策略。 */
        private ErrorStrategy errorStrategy;

        public Integer getBufferSize() {
            return bufferSize;
        }

        public void setBufferSize(Integer bufferSize) {
            this.bufferSize = bufferSize;
        }

        public ProducerType getProducerType() {
            return producerType;
        }

        public void setProducerType(ProducerType producerType) {
            this.producerType = producerType;
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

        public Boolean getDaemonThreads() {
            return daemonThreads;
        }

        public void setDaemonThreads(Boolean daemonThreads) {
            this.daemonThreads = daemonThreads;
        }

        public ErrorStrategy getErrorStrategy() {
            return errorStrategy;
        }

        public void setErrorStrategy(ErrorStrategy errorStrategy) {
            this.errorStrategy = errorStrategy;
        }
    }

    public static class Metrics {

        /** classpath 存在 Micrometer 时是否注册只读指标。 */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public enum WaitStrategyType {
        BLOCKING(BlockingWaitStrategy::new),
        BUSY_SPIN(BusySpinWaitStrategy::new),
        YIELDING(YieldingWaitStrategy::new),
        SLEEPING(SleepingWaitStrategy::new);

        private final Supplier<? extends WaitStrategy> factory;

        WaitStrategyType(Supplier<? extends WaitStrategy> factory) {
            this.factory = factory;
        }

        Supplier<? extends WaitStrategy> factory() {
            return factory;
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {

        private final String pipelineName;
        private final boolean daemon;
        private final AtomicInteger sequence = new AtomicInteger(1);

        private NamedThreadFactory(String pipelineName, boolean daemon) {
            this.pipelineName = pipelineName;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable,
                    "disruptor-" + pipelineName + "-" + sequence.getAndIncrement());
            thread.setDaemon(daemon);
            return thread;
        }
    }
}
