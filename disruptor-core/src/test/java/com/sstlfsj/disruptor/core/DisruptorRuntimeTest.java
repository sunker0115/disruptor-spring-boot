package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.Sequence;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisruptorRuntimeTest {

    private static final EventTranslatorTwoArg<TestEvent, String, Long> TRANSLATOR =
            (event, sequence, value, number) -> {
                event.value = value;
                event.number = number;
            };

    @Test
    void keepsNativeHandlerCallbacksAndTranslatorPublishing() throws Exception {
        CountDownLatch consumed = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong seenSequence = new AtomicLong(-1);
        AtomicBoolean seenEndOfBatch = new AtomicBoolean();
        AtomicBoolean batchStarted = new AtomicBoolean();
        AtomicBoolean sequenceCallbackSet = new AtomicBoolean();

        EventHandler<TestEvent> handler = new EventHandler<>() {
            @Override
            public void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
                assertEquals("order-1", event.value);
                assertEquals(42L, event.number);
                seenSequence.set(sequence);
                seenEndOfBatch.set(endOfBatch);
                consumed.countDown();
            }

            @Override
            public void onBatchStart(long batchSize, long queueDepth) {
                batchStarted.set(true);
            }

            @Override
            public void onStart() {
                started.countDown();
            }

            @Override
            public void onShutdown() {
                stopped.countDown();
            }

            @Override
            public void setSequenceCallback(Sequence sequenceCallback) {
                sequenceCallbackSet.set(sequenceCallback != null);
            }
        };

        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "orders", TestEvent.class, () -> new TestEvent("preallocated"))
                .bufferSize(16)
                .shutdownTimeout(Duration.ofSeconds(2))
                .topology(disruptor -> disruptor.handleEventsWith(handler))
                .build();

        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();
        PipelineHandle<TestEvent> handle = runtime.require("orders", TestEvent.class);

        assertSame(handle, runtime.unique(TestEvent.class));
        assertSame(handle.ringBuffer(), handle.disruptor().getRingBuffer());
        assertEquals(16, handle.ringBuffer().getBufferSize());
        assertFalse(handle.hasStarted());

        runtime.start();
        assertTrue(handle.hasStarted());
        assertTrue(started.await(2, TimeUnit.SECONDS));
        handle.ringBuffer().publishEvent(TRANSLATOR, "order-1", 42L);

        assertTrue(consumed.await(2, TimeUnit.SECONDS));
        assertEquals(0L, seenSequence.get());
        assertTrue(seenEndOfBatch.get());
        assertTrue(batchStarted.get());
        assertTrue(sequenceCallbackSet.get());

        runtime.shutdown();
        assertTrue(stopped.await(2, TimeUnit.SECONDS));
        assertFalse(runtime.isRunning());
        assertTrue(handle.hasStarted());
    }

    @Test
    void indexesPipelinesByNameAndAllowsTheSameEventType() {
        PipelineSpec<TestEvent> first = spec("first");
        PipelineSpec<TestEvent> second = spec("second");

        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .add(first)
                .add(second)
                .build();

        assertEquals(List.of("first", "second"), runtime.handles().stream()
                .map(PipelineHandle::name)
                .toList());
        assertNotNull(runtime.require("first", TestEvent.class));
        assertNotNull(runtime.require("second", TestEvent.class));
        assertThrows(IllegalStateException.class, () -> runtime.unique(TestEvent.class));
        assertThrows(IllegalArgumentException.class, () -> runtime.require("missing", TestEvent.class));
        assertThrows(IllegalArgumentException.class, () -> runtime.require("first", OtherEvent.class));
    }

    @Test
    void rejectsDuplicateNamesAndRestartAfterShutdown() {
        assertThrows(IllegalArgumentException.class, () -> DisruptorRuntime.builder()
                .add(spec("duplicate"))
                .add(spec("duplicate"))
                .build());

        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec("once")).build();
        runtime.start();
        runtime.start();
        runtime.shutdown();
        runtime.shutdown();

        assertThrows(IllegalStateException.class, runtime::start);
    }

    @Test
    void haltsStartedPipelinesWhenAStartupFails() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch firstHalted = new CountDownLatch(1);
        EventHandler<TestEvent> firstHandler = new EventHandler<>() {
            @Override
            public void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
            }

            @Override
            public void onStart() {
                firstStarted.countDown();
            }

            @Override
            public void onShutdown() {
                firstHalted.countDown();
            }
        };
        PipelineSpec<TestEvent> first = PipelineSpec.builder(
                        "first", TestEvent.class, () -> new TestEvent("slot"))
                .topology(disruptor -> disruptor.handleEventsWith(firstHandler))
                .build();
        // 第二条管道的线程工厂直接抛异常，让 disruptor.start() 失败。
        PipelineSpec<TestEvent> second = PipelineSpec.builder(
                        "second", TestEvent.class, () -> new TestEvent("slot"))
                .threadFactory(runnable -> {
                    throw new IllegalStateException("boom");
                })
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                }))
                .build();

        DisruptorRuntime runtime = DisruptorRuntime.builder().add(first).add(second).build();

        assertThrows(IllegalStateException.class, runtime::start);
        assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
        assertTrue(firstHalted.await(2, TimeUnit.SECONDS), "已启动的管道应被逆序 halt");
        assertFalse(runtime.isRunning());
    }

    @Test
    void haltsCurrentPipelineWhenAConsumerPartiallyStarts() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicInteger threadNumber = new AtomicInteger();
        AtomicReference<Thread> startedThread = new AtomicReference<>();
        EventHandler<TestEvent> firstHandler = new EventHandler<>() {
            @Override
            public void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
            }

            @Override
            public void onStart() {
                started.countDown();
            }

            @Override
            public void onShutdown() {
                stopped.countDown();
            }
        };
        PipelineSpec<TestEvent> partiallyStarted = PipelineSpec.builder(
                        "partial", TestEvent.class, () -> new TestEvent("slot"))
                .threadFactory(runnable -> {
                    if (threadNumber.incrementAndGet() == 2) {
                        throw new IllegalStateException("second consumer cannot start");
                    }
                    Thread thread = new Thread(runnable, "partial-start-1");
                    startedThread.set(thread);
                    return thread;
                })
                .topology(disruptor -> disruptor.handleEventsWith(
                        firstHandler,
                        (event, sequence, endOfBatch) -> {
                        }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(partiallyStarted).build();

        assertThrows(IllegalStateException.class, runtime::start);
        assertTrue(started.await(2, TimeUnit.SECONDS));
        assertTrue(stopped.await(2, TimeUnit.SECONDS), "部分启动的当前管道也必须被 halt");
        Thread thread = startedThread.get();
        assertNotNull(thread);
        thread.join(2_000);
        assertFalse(thread.isAlive(), "启动失败后不得残留消费线程");
        assertFalse(runtime.isRunning());
    }

    @Test
    void rejectsNullFillersBeforeClaimingARingBufferSlot() {
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec("publish-null")).build();
        PipelineHandle<TestEvent> handle = runtime.require("publish-null", TestEvent.class);
        long cursor = handle.ringBuffer().getCursor();

        assertThrows(NullPointerException.class, () -> handle.publish(null));
        assertEquals(cursor, handle.ringBuffer().getCursor());
        assertThrows(NullPointerException.class, () -> handle.tryPublish(null));
        assertEquals(cursor, handle.ringBuffer().getCursor());
    }

    @Test
    void publishesClaimedSlotWhenFillerThrows() {
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec("publish-failure")).build();
        PipelineHandle<TestEvent> handle = runtime.require("publish-failure", TestEvent.class);

        assertThrows(IllegalStateException.class, () -> handle.publish(event -> {
            throw new IllegalStateException("boom");
        }));
        assertEquals(0L, handle.ringBuffer().getCursor());
    }

    @Test
    void haltsPipelineWhenShutdownExceedsTimeout() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        EventHandler<TestEvent> blocking = (event, sequence, endOfBatch) -> {
            entered.countDown();
            release.await();
        };
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "slow", TestEvent.class, () -> new TestEvent("slot"))
                .shutdownTimeout(Duration.ofMillis(200))
                .topology(disruptor -> disruptor.handleEventsWith(blocking))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        try {
            runtime.require("slow", TestEvent.class).ringBuffer().publishEvent(TRANSLATOR, "x", 1L);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            // handler 卡死，shutdown 必须在超时后强制 halt 返回，而不是永久阻塞。
            assertTimeoutPreemptively(Duration.ofSeconds(3), runtime::shutdown);
            assertFalse(runtime.isRunning());
        } finally {
            release.countDown();
        }
    }

    @Test
    void shutsDownEveryPipelineOnStop() throws Exception {
        CountDownLatch allShutdown = new CountDownLatch(3);
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .add(shutdownSignalling("first", allShutdown))
                .add(shutdownSignalling("second", allShutdown))
                .add(shutdownSignalling("third", allShutdown))
                .build();

        runtime.start();
        assertTrue(runtime.isRunning());
        runtime.shutdown();

        // onShutdown 相对 shutdown() 返回是异步的（LMAX Disruptor 在 halt() 后不 join 消费线程），
        // 因此只断言全部管道最终关闭；逆序由 DisruptorRuntime.shutdown() 的 Collections.reverse 保证。
        assertTrue(allShutdown.await(2, TimeUnit.SECONDS), "所有管道都应触发 onShutdown");
        assertFalse(runtime.isRunning());
    }

    @Test
    void continuesStoppingOtherPipelinesWhenOneHaltFails() throws Exception {
        CountDownLatch healthyStopped = new CountDownLatch(1);
        ThrowingHaltProcessor failingProcessor = new ThrowingHaltProcessor();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .add(shutdownSignalling("healthy", healthyStopped))
                .add(PipelineSpec.builder("failing", TestEvent.class, () -> new TestEvent("slot"))
                        .topology(disruptor -> disruptor.handleEventsWith(failingProcessor))
                        .build())
                .build();

        runtime.start();
        runtime.shutdown();

        assertTrue(healthyStopped.await(2, TimeUnit.SECONDS),
                "单条管道停止失败不得阻断其余管道关闭");
        assertEquals(2, failingProcessor.haltCalls.get(),
                "shutdown 失败后应再尝试一次强制 halt");
        assertFalse(runtime.isRunning());
    }

    @Test
    void drainsInflightEventsBeforeStopping() {
        int total = 128;
        AtomicLong processed = new AtomicLong();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "drain", TestEvent.class, () -> new TestEvent("slot"))
                .bufferSize(256)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> processed.incrementAndGet()))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        var ringBuffer = runtime.require("drain", TestEvent.class).ringBuffer();
        for (int i = 0; i < total; i++) {
            ringBuffer.publishEvent(TRANSLATOR, "v", (long) i);
        }
        // shutdown 会 busy-spin 等 backlog 排空后才 halt，返回时所有事件的 onEvent 必已完成。
        runtime.shutdown();

        assertEquals(total, processed.get());
        assertFalse(runtime.isRunning());
    }

    private static PipelineSpec<TestEvent> shutdownSignalling(String name, CountDownLatch allShutdown) {
        EventHandler<TestEvent> handler = new EventHandler<>() {
            @Override
            public void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
            }

            @Override
            public void onShutdown() {
                allShutdown.countDown();
            }
        };
        return PipelineSpec.builder(name, TestEvent.class, () -> new TestEvent("slot"))
                .topology(disruptor -> disruptor.handleEventsWith(handler))
                .build();
    }

    private static PipelineSpec<TestEvent> spec(String name) {
        return PipelineSpec.builder(name, TestEvent.class, () -> new TestEvent("slot"))
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                }))
                .build();
    }

    private static final class TestEvent {
        private final String marker;
        private String value;
        private long number;

        private TestEvent(String marker) {
            this.marker = marker;
        }
    }

    private static final class OtherEvent {
    }

    private static final class ThrowingHaltProcessor implements EventProcessor {

        private final Sequence sequence = new Sequence();
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicInteger haltCalls = new AtomicInteger();

        @Override
        public Sequence getSequence() {
            return sequence;
        }

        @Override
        public void halt() {
            haltCalls.incrementAndGet();
            throw new IllegalStateException("halt failed");
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void run() {
            running.set(true);
        }
    }
}
