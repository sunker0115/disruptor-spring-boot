package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineSpecTest {

    @Test
    void programmaticOptionsOverrideRuntimeSettings() {
        ThreadFactory configuredFactory = runnable -> new Thread(runnable, "configured");
        PipelineSettings configured = PipelineSettings.builder()
                .bufferSize(64)
                .producerType(ProducerType.MULTI)
                .waitStrategy(BlockingWaitStrategy::new)
                .threadFactory(name -> configuredFactory)
                .build();

        ThreadFactory explicitFactory = runnable -> new Thread(runnable, "explicit");
        PipelineSpec<TestEvent> spec = PipelineSpec.builder("test", TestEvent.class, TestEvent::new)
                .bufferSize(32)
                .producerType(ProducerType.SINGLE)
                .waitStrategy(SleepingWaitStrategy::new)
                .threadFactory(explicitFactory)
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                }))
                .build();

        ResolvedPipelineSettings resolved = spec.resolve(configured);

        assertEquals(32, resolved.bufferSize());
        assertEquals(ProducerType.SINGLE, resolved.producerType());
        assertInstanceOf(SleepingWaitStrategy.class, resolved.waitStrategy());
        assertEquals("explicit", resolved.threadFactory().newThread(() -> {
        }).getName());
    }

    @Test
    void hasSafeNativeDefaults() {
        PipelineSettings settings = PipelineSettings.defaults();

        assertEquals(1024, settings.bufferSize());
        assertEquals(ProducerType.MULTI, settings.producerType());
        assertInstanceOf(BlockingWaitStrategy.class, settings.waitStrategyFactory().get());
        assertFalse(settings.threadFactoryFactory().apply("orders").newThread(() -> {
        }).isDaemon());
    }

    @Test
    void validatesDefinitionsAndSettings() {
        assertThrows(IllegalArgumentException.class, () -> PipelineSpec.builder(
                " ", TestEvent.class, TestEvent::new));
        assertThrows(IllegalStateException.class, () -> PipelineSpec.builder(
                "test", TestEvent.class, TestEvent::new).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineSettings.builder().bufferSize(3).build());
        assertThrows(IllegalArgumentException.class, () -> DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ZERO));
        assertTrue(PipelineSettings.isPowerOfTwo(1024));
        assertFalse(PipelineSettings.isPowerOfTwo(1023));
    }

    private static final class TestEvent {
    }
}
