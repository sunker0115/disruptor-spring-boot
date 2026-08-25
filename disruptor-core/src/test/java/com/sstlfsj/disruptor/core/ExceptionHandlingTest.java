package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.ExceptionHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 异常处理一等能力:默认安全(不卡死)、策略语义、以及 spec &gt; settings 的解析优先级。
 */
class ExceptionHandlingTest {

    private static final EventTranslatorOneArg<TestEvent, Long> TRANSLATOR =
            (event, sequence, number) -> event.number = number;

    /**
     * 毒丸回归:默认 LOG_AND_CONTINUE 下,某条事件处理抛异常必须被跳过、管道继续消费后续事件,
     * 而不是像 Disruptor 原生 FatalExceptionHandler 那样终止消费者、卡死整条管道。
     */
    @Test
    void defaultStrategySkipsFailedEventAndKeepsConsuming() throws Exception {
        CountDownLatch succeeded = new CountDownLatch(4);
        EventHandler<TestEvent> handler = (event, sequence, endOfBatch) -> {
            if (event.number == 2L) {
                throw new IllegalStateException("boom on " + event.number);
            }
            succeeded.countDown();
        };
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "resilient", TestEvent.class, TestEvent::new)
                .bufferSize(16)
                .shutdownTimeout(Duration.ofSeconds(2))
                .topology(disruptor -> disruptor.handleEventsWith(handler))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        try {
            var ringBuffer = runtime.require("resilient", TestEvent.class).ringBuffer();
            for (long i = 0; i < 5; i++) {
                ringBuffer.publishEvent(TRANSLATOR, i);
            }
            // 出错的第 3 条被跳过,其余 4 条仍被消费 → 管道未卡死。
            assertTrue(succeeded.await(2, TimeUnit.SECONDS), "出错事件后管道应继续消费,不得卡死");
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void defaultsCarryLogAndContinueHandler() {
        assertNotNull(PipelineSettings.defaults().exceptionHandler());
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
