package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;

/** 管理应用显式声明的根 EventLoop 与 EventLoopGroup，不创建业务执行器。 */
@AutoConfiguration
@ConditionalOnClass(SupervisedScheduledExecutor.class)
@ConditionalOnBean(SupervisedScheduledExecutor.class)
@ConditionalOnProperty(
        prefix = "disruptor.concurrent", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(DisruptorConcurrentProperties.class)
public class DisruptorConcurrentAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DisruptorConcurrentLifecycle disruptorConcurrentLifecycle(
            ObjectProvider<SupervisedScheduledExecutor<?>> executors,
            DisruptorConcurrentProperties properties) {
        List<SupervisedScheduledExecutor<?>> candidates = executors.orderedStream().toList();
        return new DisruptorConcurrentLifecycle(
                candidates,
                properties.getLifecyclePhase(),
                properties.getShutdownTimeout());
    }
}
