package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.ExceptionHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 异常处理一等能力：默认保护拓扑一致性、显式继续策略的原生传播语义、以及配置解析优先级。
 */
class ExceptionHandlingTest {

    private static final EventTranslatorOneArg<TestEvent, Long> TRANSLATOR =
            (event, sequence, number) -> event.number = number;

    /**
     * 默认 HALT 下，上游失败后序列不得推进，依赖它的下游不能把失败槽位当作成功结果处理。
     */
    @Test
    void defaultStrategyHaltsBeforeFailedEventReachesDownstream() throws Exception {
        CountDownLatch downstream = new CountDownLatch(1);
        EventHandler<TestEvent> upstream = (event, sequence, endOfBatch) -> {
            throw new IllegalStateException("boom on " + event.number);
        };
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                "strict", TestEvent.class, TestEvent::new)
                .bufferSize(16)
                .topology(disruptor -> disruptor.handleEventsWith(upstream)
                        .then((event, sequence, endOfBatch) -> downstream.countDown()))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ofMillis(200))
                .add(spec)
                .build();

        runtime.start();
        try {
            runtime.require("strict", TestEvent.class).publishEvent(TRANSLATOR, 1L);
            assertFalse(downstream.await(300, TimeUnit.MILLISECONDS), "失败槽位不得流向依赖它的下游");
        } finally {
            runtime.halt();
        }
    }

    @Test
    void explicitLogAndContinueAdvancesSequenceAndDownstreamSeesFailedSlot() throws Exception {
        CountDownLatch downstream = new CountDownLatch(2);
        List<Long> seen = new CopyOnWriteArrayList<>();
        EventHandler<TestEvent> upstream = (event, sequence, endOfBatch) -> {
            if (event.number == 1L) {
                throw new IllegalStateException("boom on " + event.number);
            }
        };
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "available", TestEvent.class, TestEvent::new)
                .bufferSize(16)
                .exceptionHandler(ErrorStrategy.LOG_AND_CONTINUE.handler())
                .topology(disruptor -> disruptor.handleEventsWith(upstream)
                        .then((event, sequence, endOfBatch) -> {
                            seen.add(event.number);
                            downstream.countDown();
                        }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        try {
            var pipeline = runtime.require("available", TestEvent.class);
            pipeline.publishEvent(TRANSLATOR, 1L);
            pipeline.publishEvent(TRANSLATOR, 2L);

            assertTrue(downstream.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(1L, 2L), seen);
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void defaultsCarryHaltHandler() {
        assertNotNull(PipelineSettings.defaults().exceptionHandler());
        assertThrows(RuntimeException.class, () -> PipelineSettings.defaults().exceptionHandler()
                .handleEventException(new IllegalStateException("x"), 0L, new Object()));
    }

    @Test
    void logAndContinueSwallowsWhileHaltRethrows() {
        Throwable cause = new IllegalStateException("x");
        assertDoesNotThrow(() ->
                ErrorStrategy.LOG_AND_CONTINUE.handler().handleEventException(cause, 0L, new Object()));
        assertThrows(RuntimeException.class, () ->
                ErrorStrategy.HALT.handler().handleEventException(cause, 0L, new Object()));
    }

    @Test
    void specExceptionHandlerOverridesSettings() {
        ExceptionHandler<Object> fromSettings = ErrorStrategy.HALT.handler();
        ExceptionHandler<TestEvent> fromSpec = new NoopExceptionHandler();
        PipelineSettings settings = PipelineSettings.builder().exceptionHandler(fromSettings).build();

        PipelineSpec<TestEvent> withSpec = PipelineSpec.builder("a", TestEvent.class, TestEvent::new)
                .exceptionHandler(fromSpec)
                .topology(noopTopology())
                .build();
        PipelineSpec<TestEvent> withoutSpec = PipelineSpec.builder("b", TestEvent.class, TestEvent::new)
                .topology(noopTopology())
                .build();

        assertSame(fromSpec, withSpec.resolve(settings).exceptionHandler());
        assertSame(fromSettings, withoutSpec.resolve(settings).exceptionHandler());
    }

    private static DisruptorTopology<TestEvent> noopTopology() {
        return disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
        });
    }

    private static final class NoopExceptionHandler implements ExceptionHandler<TestEvent> {
        @Override
        public void handleEventException(Throwable ex, long sequence, TestEvent event) {
        }

        @Override
        public void handleOnStartException(Throwable ex) {
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
        }
    }

    private static final class TestEvent {
        private long number;
    }
}
