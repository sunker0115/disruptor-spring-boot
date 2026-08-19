package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
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

import java.util.concurrent.ThreadFactory;

/**
 * Auto-configuration for the disruptor-based asynchronous event bus.
 *
 * <p>Wires a {@link Disruptor} backed by a multi-producer ring buffer with the
 * configured size and wait strategy, registers a single {@link EventHandler}
 * that routes each event to the {@link ConsumerRegistry} by runtime type, and
 * exposes the public beans. The disruptor is <em>not</em> started here: its
 * start/stop is driven by {@link DisruptorLifecycle} (a {@link SmartLifecycle}),
 * so consumer threads start only after the context is ready and shut down in an
 * orderly fashion with a bounded drain timeout.</p>
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
    public Disruptor<EventWrapper> disruptor(DisruptorProperties properties,
                                             ConsumerRegistry consumerRegistry) {
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

        disruptor.handleEventsWith((wrapper, sequence, endOfBatch) -> {
            try {
                consumerRegistry.dispatch(wrapper.getType(), wrapper.getPayload());
            } finally {
                // 清空槽位，避免 RingBuffer 在槽位被复用前一直持有大 payload 引用。
                wrapper.clear();
            }
        });

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
    public DisruptorListenerRegistrar disruptorListenerRegistrar(ConsumerRegistry consumerRegistry,
                                                                 ConfigurableListableBeanFactory beanFactory) {
        return new DisruptorListenerRegistrar(consumerRegistry, beanFactory);
    }
}
