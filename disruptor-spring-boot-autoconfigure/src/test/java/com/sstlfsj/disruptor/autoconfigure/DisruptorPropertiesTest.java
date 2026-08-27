package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.core.ErrorStrategy;
import com.sstlfsj.disruptor.core.PipelineSettings;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DisruptorPropertiesTest {

    @Test
    void resolvesSafeDefaults() {
        PipelineSettings settings = new DisruptorProperties().settingsFor("orders");

        assertThat(settings.bufferSize()).isEqualTo(1024);
        assertThat(settings.producerType()).isEqualTo(ProducerType.MULTI);
        assertThat(settings.waitStrategyFactory().get()).isInstanceOf(BlockingWaitStrategy.class);
        assertThat(new DisruptorProperties().getShutdownTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(settings.threadFactoryFactory().apply("orders").newThread(() -> {
        }).isDaemon()).isFalse();
        assertThatThrownBy(() -> settings.exceptionHandler()
                .handleEventException(new IllegalStateException("boom"), 0L, new Object()))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void namedPipelineOverridesEveryPipelineSetting() {
        DisruptorProperties properties = new DisruptorProperties();
        DisruptorProperties.Pipeline override = new DisruptorProperties.Pipeline();
        override.setBufferSize(128);
        override.setProducerType(ProducerType.SINGLE);
        override.setWaitStrategy(DisruptorProperties.WaitStrategyType.YIELDING);
        override.setDaemonThreads(true);
        override.setErrorStrategy(ErrorStrategy.LOG_AND_CONTINUE);
        properties.getPipelines().put("orders", override);

        PipelineSettings settings = properties.settingsFor("orders");

        assertThat(settings.bufferSize()).isEqualTo(128);
        assertThat(settings.producerType()).isEqualTo(ProducerType.SINGLE);
        assertThat(settings.waitStrategyFactory().get()).isInstanceOf(YieldingWaitStrategy.class);
        Thread thread = settings.threadFactoryFactory().apply("orders").newThread(() -> {
        });
        assertThat(thread.getName()).isEqualTo("disruptor-orders-1");
        assertThat(thread.isDaemon()).isTrue();
        settings.exceptionHandler().handleEventException(
                new IllegalStateException("boom"), 0L, new Object());
    }
}
