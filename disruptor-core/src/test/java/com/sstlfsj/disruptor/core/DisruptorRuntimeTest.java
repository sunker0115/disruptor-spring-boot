package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
                .topology(disruptor -> disruptor.handleEventsWith(handler))
                .build();

        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ofSeconds(2))
                .add(spec)
                .build();
        PipelineHandle<TestEvent> handle = runtime.require("orders", TestEvent.class);

        assertEquals(handle, runtime.unique(TestEvent.class));
        assertEquals(16, handle.unsafeRingBuffer().getBufferSize());

        runtime.start();
        assertTrue(started.await(2, TimeUnit.SECONDS));
        handle.publishEvent(TRANSLATOR, "order-1", 42L);

        assertTrue(consumed.await(2, TimeUnit.SECONDS));
        assertEquals(0L, seenSequence.get());
        assertTrue(seenEndOfBatch.get());
        assertTrue(batchStarted.get());
        assertTrue(sequenceCallbackSet.get());

        runtime.shutdown();
        assertEquals(0L, stopped.getCount(), "shutdown 返回前消费线程必须完成 onShutdown");
        assertFalse(runtime.isRunning());
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
        long cursor = handle.unsafeRingBuffer().getCursor();

        assertThrows(NullPointerException.class, () -> handle.publish(null));
        assertEquals(cursor, handle.unsafeRingBuffer().getCursor());
        assertThrows(NullPointerException.class, () -> handle.tryPublish(null));
        assertEquals(cursor, handle.unsafeRingBuffer().getCursor());
    }

    @Test
    void releasesSinglePublisherAdmissionWhenFillerThrows() {
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "publish-failure", TestEvent.class, () -> new TestEvent("slot"))
                .producerType(ProducerType.SINGLE)
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ofMillis(200))
                .add(spec)
                .build();
        PipelineHandle<TestEvent> handle = runtime.require("publish-failure", TestEvent.class);

        runtime.start();
        assertThrows(IllegalStateException.class, () -> handle.publish(event -> {
            throw new IllegalStateException("boom");
        }));
        assertEquals(0L, handle.unsafeRingBuffer().getCursor());

        runtime.shutdown();
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
                .topology(disruptor -> disruptor.handleEventsWith(blocking))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ofMillis(200))
                .add(spec)
                .build();

        runtime.start();
        try {
            runtime.require("slow", TestEvent.class).publishEvent(TRANSLATOR, "x", 1L);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            // handler 卡死，shutdown 必须在超时后强制 halt 返回，而不是永久阻塞。
            assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
                    assertThrows(DisruptorShutdownException.class, runtime::shutdown));
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

        assertEquals(0L, allShutdown.getCount(), "shutdown 返回前全部消费线程应退出");
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
        assertThrows(DisruptorShutdownException.class, runtime::shutdown);

        assertTrue(healthyStopped.await(2, TimeUnit.SECONDS),
                "单条管道停止失败不得阻断其余管道关闭");
        assertEquals(1, failingProcessor.haltCalls.get(),
                "同一次关闭不得重复调用失败的 halt");
        assertFalse(runtime.isRunning());
    }

    @RepeatedTest(20)
    void drainsInflightEventsEvenWhenConsumerThreadHasNotEnteredRun() throws Exception {
        int total = 128;
        CountDownLatch consumerThreadCreated = new CountDownLatch(1);
        CountDownLatch allowConsumerToRun = new CountDownLatch(1);
        AtomicLong processed = new AtomicLong();
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "drain", TestEvent.class, () -> new TestEvent("slot"))
                .bufferSize(256)
                .threadFactory(runnable -> new Thread(() -> {
                    consumerThreadCreated.countDown();
                    try {
                        allowConsumerToRun.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    runnable.run();
                }, "delayed-consumer"))
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> processed.incrementAndGet()))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        assertTrue(consumerThreadCreated.await(2, TimeUnit.SECONDS));
        PipelineHandle<TestEvent> handle = runtime.require("drain", TestEvent.class);
        for (int i = 0; i < total; i++) {
            handle.publishEvent(TRANSLATOR, "v", (long) i);
        }

        Thread shutdown = Thread.ofPlatform().name("delayed-consumer-shutdown").start(() -> {
            try {
                runtime.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        });
        awaitCondition(() -> !runtime.isRunning(), Duration.ofSeconds(2));
        allowConsumerToRun.countDown();
        shutdown.join(2_000);

        assertFalse(shutdown.isAlive());
        assertEquals(null, shutdownFailure.get());
        assertEquals(total, processed.get());
        assertFalse(runtime.isRunning());
    }

    @ParameterizedTest
    @EnumSource(ProducerType.class)
    void waitsForInFlightPublicationBeforeCapturingDrainTarget(ProducerType producerType)
            throws Exception {
        CountDownLatch translating = new CountDownLatch(1);
        CountDownLatch releaseTranslator = new CountDownLatch(1);
        CountDownLatch consumed = new CountDownLatch(1);
        AtomicReference<Throwable> publisherFailure = new AtomicReference<>();
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "publication-boundary", TestEvent.class, () -> new TestEvent("slot"))
                .producerType(producerType)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> consumed.countDown()))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();
        PipelineHandle<TestEvent> handle = runtime.require("publication-boundary", TestEvent.class);
        runtime.start();

        Thread publisher = Thread.ofPlatform().name("test-publisher").start(() -> {
            try {
                handle.publishEvent((event, sequence) -> {
                    translating.countDown();
                    try {
                        releaseTranslator.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("发布线程被中断", interrupted);
                    }
                    event.value = "accepted";
                });
            } catch (Throwable failure) {
                publisherFailure.set(failure);
            }
        });
        assertTrue(translating.await(2, TimeUnit.SECONDS));

        Thread shutdown = Thread.ofPlatform().name("test-shutdown").start(() -> {
            try {
                runtime.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        });
        awaitCondition(() -> !runtime.isRunning(), Duration.ofSeconds(2));

        releaseTranslator.countDown();
        publisher.join(2_000);
        shutdown.join(2_000);

        assertFalse(publisher.isAlive());
        assertFalse(shutdown.isAlive());
        assertEquals(null, publisherFailure.get());
        assertEquals(null, shutdownFailure.get());
        assertEquals(0L, consumed.getCount(), "关闭边界前进入的发布必须被消费");
    }

    @Test
    void waitsForEveryInFlightMultiProducerBeforeCapturingDrainTarget() throws Exception {
        int publisherCount = 4;
        CountDownLatch translating = new CountDownLatch(publisherCount);
        CountDownLatch releaseTranslators = new CountDownLatch(1);
        CountDownLatch consumed = new CountDownLatch(publisherCount);
        AtomicReference<Throwable> publisherFailure = new AtomicReference<>();
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "multi-publication-boundary", TestEvent.class, () -> new TestEvent("slot"))
                .bufferSize(16)
                .producerType(ProducerType.MULTI)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> consumed.countDown()))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();
        PipelineHandle<TestEvent> handle = runtime.require(
                "multi-publication-boundary", TestEvent.class);
        runtime.start();

        Thread[] publishers = new Thread[publisherCount];
        for (int index = 0; index < publisherCount; index++) {
            publishers[index] = Thread.ofPlatform().name("multi-publisher-" + index).start(() -> {
                try {
                    handle.publishEvent((event, sequence) -> {
                        translating.countDown();
                        try {
                            releaseTranslators.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("发布线程被中断", interrupted);
                        }
                        event.number = sequence;
                    });
                } catch (Throwable failure) {
                    publisherFailure.compareAndSet(null, failure);
                }
            });
        }
        assertTrue(translating.await(2, TimeUnit.SECONDS));

        Thread shutdown = Thread.ofPlatform().name("multi-publisher-shutdown").start(() -> {
            try {
                runtime.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        });
        awaitCondition(() -> !handle.tryPublishEvent(TRANSLATOR, "probe", 1L),
                Duration.ofSeconds(2));

        releaseTranslators.countDown();
        for (Thread publisher : publishers) {
            publisher.join(2_000);
            assertFalse(publisher.isAlive());
        }
        shutdown.join(2_000);

        assertFalse(shutdown.isAlive());
        assertEquals(null, publisherFailure.get());
        assertEquals(null, shutdownFailure.get());
        assertEquals(0L, consumed.getCount());
    }

    @Test
    void quiescesEveryPipelineBeforeDrainingAnyPipeline() throws Exception {
        CountDownLatch secondHandlerEntered = new CountDownLatch(1);
        CountDownLatch releaseSecondHandler = new CountDownLatch(1);
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        PipelineSpec<TestEvent> second = PipelineSpec.builder(
                        "second", TestEvent.class, () -> new TestEvent("slot"))
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                    secondHandlerEntered.countDown();
                    releaseSecondHandler.await();
                }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ofSeconds(2))
                .add(spec("first"))
                .add(second)
                .build();
        PipelineHandle<TestEvent> firstHandle = runtime.require("first", TestEvent.class);
        PipelineHandle<TestEvent> secondHandle = runtime.require("second", TestEvent.class);

        runtime.start();
        secondHandle.publishEvent(TRANSLATOR, "blocking", 1L);
        assertTrue(secondHandlerEntered.await(2, TimeUnit.SECONDS));

        Thread shutdown = Thread.ofPlatform().name("multi-pipeline-shutdown").start(() -> {
            try {
                runtime.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        });
        awaitCondition(() -> !secondHandle.tryPublishEvent(TRANSLATOR, "probe", 2L),
                Duration.ofSeconds(2));

        assertFalse(firstHandle.tryPublishEvent(TRANSLATOR, "late", 3L),
                "排空任一管道前必须先关闭全部受管发布入口");

        releaseSecondHandler.countDown();
        shutdown.join(2_000);

        assertFalse(shutdown.isAlive());
        assertEquals(null, shutdownFailure.get());
    }

    @Test
    void rejectsManagedPublishingOutsideRunningState() {
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec("managed-state")).build();
        PipelineHandle<TestEvent> handle = runtime.require("managed-state", TestEvent.class);

        assertFalse(handle.tryPublishEvent(TRANSLATOR, "before", 1L));
        assertThrows(IllegalStateException.class,
                () -> handle.publishEvent(TRANSLATOR, "before", 1L));

        runtime.start();
        assertTrue(handle.tryPublishEvent(TRANSLATOR, "running", 2L));
        runtime.shutdown();

        assertFalse(handle.tryPublishEvent(TRANSLATOR, "after", 3L));
        assertThrows(IllegalStateException.class,
                () -> handle.publishEvent(TRANSLATOR, "after", 3L));
    }

    @Test
    void supportsManagedTranslatorOverloads() throws Exception {
        CountDownLatch consumed = new CountDownLatch(4);
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "translator-overloads", TestEvent.class, () -> new TestEvent("slot"))
                .producerType(ProducerType.SINGLE)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> consumed.countDown()))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();
        PipelineHandle<TestEvent> handle = runtime.require("translator-overloads", TestEvent.class);
        EventTranslator<TestEvent> zero = (event, sequence) -> event.number = 0L;
        EventTranslatorOneArg<TestEvent, Long> one =
                (event, sequence, number) -> event.number = number;
        EventTranslatorThreeArg<TestEvent, String, Long, Boolean> three =
                (event, sequence, value, number, ignored) -> {
                    event.value = value;
                    event.number = number;
                };

        runtime.start();
        handle.publishEvent(zero);
        assertTrue(handle.tryPublishEvent(one, 1L));
        handle.publishEvent(TRANSLATOR, "two", 2L);
        assertTrue(handle.tryPublishEvent(three, "three", 3L, true));

        assertTrue(consumed.await(2, TimeUnit.SECONDS));
        runtime.shutdown();
    }

    private static void awaitCondition(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("等待条件超时");
            }
            LockSupport.parkNanos(100_000L);
        }
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
