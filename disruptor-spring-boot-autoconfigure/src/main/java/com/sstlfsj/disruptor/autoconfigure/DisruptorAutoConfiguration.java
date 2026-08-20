package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.core.DefaultEventBus;
import com.sstlfsj.disruptor.core.DisruptorConfig;
import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.core.PipelineBuilder;
import com.sstlfsj.disruptor.core.Pipelines;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration（spring-boot 层）：把 core 的构建/发布组件与 spring 的扫描/生命周期组件
 * 装配为 bean。装配链：{@link DisruptorProperties} → {@link DisruptorConfig} → {@link PipelineBuilder}；
 * {@link StagePipelineRegistrar} 启动时扫描声明式/编程式定义交给 builder 建管道、注册进 {@link Pipelines}；
 * {@link EventBus} 发布门面；{@link DisruptorLifecycle} 托管所有管道启停。
 *
 * <p>依赖方向 autoconfigure → spring → core，三层可按需拆为独立 Maven 模块。</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(DisruptorProperties.class)
public class DisruptorAutoConfiguration {

    private static final int LIFECYCLE_PHASE = Integer.MIN_VALUE;

    @Bean
    @ConditionalOnMissingBean
    public DisruptorConfig disruptorConfig(DisruptorProperties properties) {
        return properties.toConfig();
    }

    @Bean
    @ConditionalOnMissingBean
    public Pipelines disruptorPipelines() {
        return new Pipelines();
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelineBuilder disruptorPipelineBuilder(DisruptorConfig config) {
        return new PipelineBuilder(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public StagePipelineRegistrar disruptorStagePipelineRegistrar(ConfigurableListableBeanFactory beanFactory,
                                                                  PipelineBuilder pipelineBuilder,
                                                                  Pipelines pipelines) {
        return new StagePipelineRegistrar(beanFactory, pipelineBuilder, pipelines);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventBus eventBus(Pipelines pipelines) {
        return new DefaultEventBus(pipelines);
    }

    @Bean
    @ConditionalOnMissingBean
    public DisruptorLifecycle disruptorLifecycle(Pipelines pipelines, DisruptorConfig config) {
        return new DisruptorLifecycle(pipelines, config.shutdownTimeout(), LIFECYCLE_PHASE);
    }
}
