package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.event.EventBus;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the disruptor-based typed event pipelines.
 *
 * <p>每种事件类型对应一条强类型管道（一个 {@code Disruptor<E>}，预分配事件、原地 mutate、
 * 零分配发布）。处理阶段由 {@link com.sstlfsj.disruptor.event.DisruptorStage @DisruptorStage}
 * 声明，{@link PipelineRegistrar} 在启动时扫描组装、按依赖编排成 DAG。发布经
 * {@link EventBus} 门面按事件类型定位管道。所有管道的启停由 {@link DisruptorLifecycle} 托管。</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(DisruptorProperties.class)
public class DisruptorAutoConfiguration {

    /**
     * SmartLifecycle phase for the event bus. Kept at the minimum so pipelines start first and
     * stop last — after upstream sources (web server, MQ listeners, etc.) have stopped producing.
     */
    private static final int LIFECYCLE_PHASE = Integer.MIN_VALUE;

    @Bean
    @ConditionalOnMissingBean
    public Pipelines disruptorPipelines() {
        return new Pipelines();
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelineRegistrar disruptorPipelineRegistrar(ConfigurableListableBeanFactory beanFactory,
                                                        DisruptorProperties properties,
                                                        Pipelines pipelines) {
        return new PipelineRegistrar(beanFactory, properties, pipelines);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventBus eventBus(Pipelines pipelines) {
        return new DefaultEventBus(pipelines);
    }

    @Bean
    @ConditionalOnMissingBean
    public SmartLifecycle disruptorLifecycle(Pipelines pipelines, DisruptorProperties properties) {
        return new DisruptorLifecycle(pipelines, properties.getShutdownTimeout(), LIFECYCLE_PHASE);
    }
}
