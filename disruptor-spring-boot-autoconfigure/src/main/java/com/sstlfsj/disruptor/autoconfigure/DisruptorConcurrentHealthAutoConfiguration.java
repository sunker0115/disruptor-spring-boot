package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

/** Boot Health 存在时装配 concurrent 根对象健康贡献者。 */
@AutoConfiguration(after = DisruptorConcurrentAutoConfiguration.class)
@ConditionalOnClass({SupervisedScheduledExecutor.class, HealthIndicator.class})
@ConditionalOnBean(DisruptorConcurrentLifecycle.class)
public class DisruptorConcurrentHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DisruptorConcurrentHealthContributor disruptorConcurrentHealthContributor(
            DisruptorConcurrentLifecycle lifecycle) {
        return new DisruptorConcurrentHealthContributor(lifecycle.managedExecutors());
    }
}
