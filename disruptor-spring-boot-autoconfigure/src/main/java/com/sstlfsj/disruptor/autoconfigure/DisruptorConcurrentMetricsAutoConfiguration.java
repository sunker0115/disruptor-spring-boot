package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/** Micrometer 存在时装配 concurrent EventLoop 快照指标。 */
@AutoConfiguration(
        after = DisruptorConcurrentAutoConfiguration.class,
        afterName = {
                "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
                "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration"
        })
@ConditionalOnClass({SupervisedScheduledExecutor.class, MeterRegistry.class})
@ConditionalOnBean({DisruptorConcurrentLifecycle.class, MeterRegistry.class})
public class DisruptorConcurrentMetricsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DisruptorConcurrentMetrics disruptorConcurrentMetrics(
            DisruptorConcurrentLifecycle lifecycle) {
        return new DisruptorConcurrentMetrics(lifecycle.managedExecutors());
    }
}
