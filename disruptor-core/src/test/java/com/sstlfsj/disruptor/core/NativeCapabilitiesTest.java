package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RewindableException;
import com.lmax.disruptor.RewindableEventHandler;
import com.lmax.disruptor.SimpleBatchRewindStrategy;
import com.lmax.disruptor.dsl.EventProcessorFactory;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeCapabilitiesTest {

    private static final EventTranslatorOneArg<TestEvent, Long> TRANSLATOR =
            (event, sequence, value) -> event.value = value;

    @Test
    void keepsNativeDefaultExceptionHandlingSemantics() throws Exception {
        CountDownLatch exceptionHandled = new CountDownLatch(1);
        CountDownLatch nextEventHandled = new CountDownLatch(1);
        AtomicLong failedSequence = new AtomicLong(-1);
        TestEvent[] failedEvent = new TestEvent[1];

        ExceptionHandler<TestEvent> exceptionHandler = new ExceptionHandler<>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, TestEvent event) {
                failedSequence.set(sequence);
                failedEvent[0] = event;
                exceptionHandled.countDown();
            }

            @Override
            public void handleOnStartException(Throwable ex) {
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
            }
        };
        EventHandler<TestEvent> handler = (event, sequence, endOfBatch) -> {
            if (event.value == 1L) {
                throw new IllegalStateException("expected");
            }
            nextEventHandled.countDown();
        };

        PipelineSpec<TestEvent> spec = PipelineSpec.builder("exceptions", TestEvent.class, TestEvent::new)
                .exceptionHandler(exceptionHandler)
                .topology(disruptor -> disruptor.handleEventsWith(handler))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        try {
            var ringBuffer = runtime.require("exceptions", TestEvent.class).unsafeRingBuffer();
            ringBuffer.publishEvent(TRANSLATOR, 1L);
            ringBuffer.publishEvent(TRANSLATOR, 2L);

            assertTrue(exceptionHandled.await(2, TimeUnit.SECONDS));
            assertTrue(nextEventHandled.await(2, TimeUnit.SECONDS));
            assertEquals(0L, failedSequence.get());
            assertSame(ringBuffer.get(0), failedEvent[0]);
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void supportsNativeBatchRewindWithoutAnAdapter() throws Exception {
        CountDownLatch handled = new CountDownLatch(1);
        AtomicBoolean firstAttempt = new AtomicBoolean(true);
        RewindableEventHandler<TestEvent> handler = (event, sequence, endOfBatch) -> {
            if (firstAttempt.compareAndSet(true, false)) {
                throw new RewindableException(new IllegalStateException("retry"));
            }
            handled.countDown();
        };

        PipelineSpec<TestEvent> spec = PipelineSpec.builder("rewind", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(
                        new SimpleBatchRewindStrategy(), handler))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        try {
            runtime.require("rewind", TestEvent.class).unsafeRingBuffer().publishEvent(TRANSLATOR, 1L);
            assertTrue(handled.await(2, TimeUnit.SECONDS));
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void supportsNativeEventProcessorFactory() throws Exception {
        CountDownLatch handled = new CountDownLatch(1);
        EventProcessorFactory<TestEvent> processorFactory = (ringBuffer, barrierSequences) ->
                new BatchEventProcessorBuilder().build(
                        ringBuffer,
                        ringBuffer.newBarrier(barrierSequences),
                        (event, sequence, endOfBatch) -> handled.countDown());
        PipelineSpec<TestEvent> spec = PipelineSpec.builder("processor", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(processorFactory))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        try {
            runtime.require("processor", TestEvent.class).unsafeRingBuffer().publishEvent(TRANSLATOR, 1L);
            assertTrue(handled.await(2, TimeUnit.SECONDS));
        } finally {
            runtime.shutdown();
        }
    }

    private static final class TestEvent {
        private long value;
    }
}
