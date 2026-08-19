package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.event.ConsumerRegistry;
import com.sstlfsj.disruptor.event.DefaultConsumerRegistry;
import com.sstlfsj.disruptor.event.EventPublisher;
import com.sstlfsj.disruptor.event.EventWrapper;
import com.sstlfsj.disruptor.event.LoggingExceptionHandler;
import com.sstlfsj.disruptor.event.RingBufferEventPublisher;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * Auto-configuration for the disruptor-based asynchronous event bus.
 *
 * <p>事件总线由若干<em>处理阶段（Stage）</em>组成：每个阶段是一个 Disruptor
 * {@link EventHandler}（看到流经 ring buffer 的每个事件），阶段内部按事件运行时类型
 * 路由分发给订阅者；阶段之间按 {@link DisruptorProperties#getPipeline() pipeline} 声明的
 * 依赖用 {@code then/and} 编排成 DAG。未配置 pipeline 时只有隐式的 {@code default}
 * 单阶段，行为与简单类型路由总线一致。</p>
 *
 * <p>Disruptor 的启停由 {@link DisruptorLifecycle}（{@link SmartLifecycle}）托管，
 * 消费线程在上下文就绪后启动、关闭时有序排空并带超时兜底。</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(DisruptorProperties.class)
public class DisruptorAutoConfiguration {

    /**
     * SmartLifecycle phase for the event bus. Kept at the minimum so the bus
     * starts first and stops last — i.e. after upstream sources (web server,
     * MQ listeners, etc.) have already stopped producing events.
     */
    private static final int LIFECYCLE_PHASE = Integer.MIN_VALUE;

    @Bean
    @ConditionalOnMissingBean
    public ConsumerRegistry consumerRegistry() {
        return new DefaultConsumerRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelineTopology disruptorPipelineTopology(DisruptorProperties properties) {
        Map<String, List<String>> after = new LinkedHashMap<>();
        properties.getPipeline().forEach((name, def) -> after.put(name, def.getAfter()));
        return PipelineTopology.build(after);
    }

    @Bean
    @ConditionalOnMissingBean
    public StageRegistries disruptorStageRegistries(PipelineTopology topology,
                                                    ConsumerRegistry consumerRegistry) {
        return new StageRegistries(topology, consumerRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public Disruptor<EventWrapper> disruptor(DisruptorProperties properties,
                                             PipelineTopology topology,
                                             StageRegistries stageRegistries) {
        EventFactory<EventWrapper> eventFactory = EventWrapper::new;
        WaitStrategy waitStrategy = properties.createWaitStrategy();

        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "disruptor-event-bus");
            thread.setDaemon(true);
            return thread;
        };

        Disruptor<EventWrapper> disruptor = new Disruptor<>(
                eventFactory,
                properties.getBufferSize(),
                threadFactory,
                ProducerType.MULTI,
                waitStrategy);

        // 兜底异常处理：替换默认 FatalExceptionHandler，避免消费者异常杀死消费线程。
        disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());

        // 按拓扑顺序为每个阶段注册一个 EventHandler，并用 then/and 表达阶段间依赖。
        boolean singleStage = topology.isSingleStage();
        Map<String, EventHandlerGroup<EventWrapper>> groups = new HashMap<>();
        for (String stage : topology.order()) {
            ConsumerRegistry registry = stageRegistries.forStage(stage);
            EventHandler<EventWrapper> handler = (wrapper, sequence, endOfBatch) -> {
                try {
                    registry.dispatch(wrapper.getType(), wrapper.getPayload());
                } finally {
                    // 仅单阶段时内联清空槽位（避免大 payload 引用滞留）；多阶段时下游仍需读取
                    // 同一 payload，且多个并行叶子阶段并发清空会有数据竞争，故不清空。
                    if (singleStage) {
                        wrapper.clear();
                    }
                }
            };
            List<String> deps = topology.dependenciesOf(stage);
            EventHandlerGroup<EventWrapper> group;
            if (deps.isEmpty()) {
                group = disruptor.handleEventsWith(handler);
            } else {
                EventHandlerGroup<EventWrapper> barrier = null;
                for (String dep : deps) {
                    EventHandlerGroup<EventWrapper> depGroup = groups.get(dep);
                    barrier = (barrier == null) ? depGroup : barrier.and(depGroup);
                }
                group = barrier.handleEventsWith(handler);
            }
            groups.put(stage, group);
        }

        return disruptor;
    }

    @Bean
    @ConditionalOnMissingBean
    public SmartLifecycle disruptorLifecycle(Disruptor<EventWrapper> disruptor,
                                             DisruptorProperties properties) {
        return new DisruptorLifecycle(disruptor, properties.getShutdownTimeout(), LIFECYCLE_PHASE);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventPublisher eventPublisher(Disruptor<EventWrapper> disruptor) {
        return new RingBufferEventPublisher(disruptor.getRingBuffer());
    }

    @Bean
    @ConditionalOnMissingBean
    public DisruptorListenerRegistrar disruptorListenerRegistrar(StageRegistries stageRegistries,
                                                                 ConfigurableListableBeanFactory beanFactory) {
        return new DisruptorListenerRegistrar(stageRegistries, beanFactory);
    }
}
