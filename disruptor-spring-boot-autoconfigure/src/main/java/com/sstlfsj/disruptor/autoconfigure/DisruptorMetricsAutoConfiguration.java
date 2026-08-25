package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.core.DisruptorRuntime;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/** Micrometer 存在时注册只读 Disruptor 运行指标。 */
@AutoConfiguration(
        after = DisruptorAutoConfiguration.class,
        afterName = {
                "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
                "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration"
        })
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnBean({DisruptorRuntime.class, MeterRegistry.class})
@ConditionalOnProperty(prefix = "disruptor.metrics", name = "enabled", matchIfMissing = true)
public class DisruptorMetricsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DisruptorMetrics disruptorMetrics(DisruptorRuntime runtime) {
        return new DisruptorMetrics(runtime);
    }
}
