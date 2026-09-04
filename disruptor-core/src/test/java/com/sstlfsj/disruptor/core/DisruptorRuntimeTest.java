package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisruptorRuntimeTest {

    private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(2);
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
        assertEquals(PublicationResult.PUBLISHED,
                handle.publishEvent(TRANSLATOR, "order-1", 42L, PUBLISH_TIMEOUT));

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
    void shutdownBeforeRuntimeStartTerminatesEveryPipeline() throws Exception {
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .add(spec("never-started-first"))
                .add(spec("never-started-second"))
                .build();

        runtime.shutdown();

        runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(Set.of(SupervisedLifecycle.TERMINATED), runtime.pipelines().stream()
                .map(DisruptorPipeline::snapshot)
                .map(PipelineSnapshot::lifecycle)
                .collect(java.util.stream.Collectors.toSet()));
        assertThrows(IllegalStateException.class, runtime::start);
    }

    @Test
    void shutdownBeforeStartAlsoTerminatesAnEmptyRuntime() throws Exception {
        DisruptorRuntime runtime = DisruptorRuntime.builder().build();

        runtime.shutdown();

        runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThrows(IllegalStateException.class, runtime::start);
    }

    @Test
    void startupRollbackAndLaterStopCallsReuseOneShutdownSessionDeadline() throws Throwable {
        IllegalStateException startupFailure = new IllegalStateException("second start failed");
        StubPipeline first = new StubPipeline("rollback-blocked");
        StubPipeline second = new StubPipeline("rollback-failed")
                .failStart(startupFailure)
                .terminateOnRequest();
        DisruptorRuntime runtime = new DisruptorRuntime(
                List.of(first, second), Duration.ofSeconds(2));
        CompletionStage<Void> start = runtime.startAsync();
        Throwable testFailure = null;
        try {
            awaitCondition(() -> first.deadlines.size() == 1 && second.deadlines.size() == 1,
                    Duration.ofSeconds(2));
            ShutdownDeadline rollbackDeadline = first.deadlines.get(0);

            CompletionStage<Void> halt = runtime.haltAsync();
            CompletionStage<Void> repeated = runtime.shutdownAsync();

            assertSame(halt, repeated);
            assertEquals(3, first.deadlines.size());
            assertEquals(3, second.deadlines.size());
            assertTrue(first.deadlines.stream().allMatch(deadline -> deadline == rollbackDeadline));
            assertTrue(second.deadlines.stream().allMatch(deadline -> deadline == rollbackDeadline));
            assertEquals(rollbackDeadline.deadlineNanos(),
                    second.deadlines.get(0).deadlineNanos());

            first.completeTermination(ShutdownMode.IMMEDIATE, null);
            assertThrows(CompletionException.class, () -> start.toCompletableFuture().join());
            assertThrows(CompletionException.class, () -> halt.toCompletableFuture().join());
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            try {
                first.completeTermination(ShutdownMode.IMMEDIATE, null);
                second.completeTermination(ShutdownMode.IMMEDIATE, startupFailure);
                runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            } catch (Throwable cleanupFailure) {
                if (testFailure == null) {
                    throw cleanupFailure;
                }
                testFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    @Test
    void deadlineOutcomeDoesNotWaitForAStuckStartupRollback() throws Exception {
        Duration shutdownBudget = Duration.ofMillis(150);
        CountDownLatch threadFactoryEntered = new CountDownLatch(1);
        CountDownLatch releaseThreadFactory = new CountDownLatch(1);
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .shutdownTimeout(shutdownBudget)
                .add(PipelineSpec.builder("stuck-startup", TestEvent.class,
                                () -> new TestEvent("slot"))
                        .threadFactory(runnable -> {
                            threadFactoryEntered.countDown();
                            awaitUninterruptibly(releaseThreadFactory);
                            return new Thread(runnable, "stuck-startup-worker");
                        })
                        .topology(disruptor -> disruptor.handleEventsWith(
                                (event, sequence, endOfBatch) -> {
                                }))
                        .build())
                .build();
        CompletionStage<Void> start = runtime.startAsync();
        try {
            assertTrue(threadFactoryEntered.await(2, TimeUnit.SECONDS));
            long requestedAt = System.nanoTime();
            CompletionStage<Void> outcome = runtime.shutdownAsync();

            ExecutionException timeout = assertThrows(ExecutionException.class,
                    () -> outcome.toCompletableFuture().get(1, TimeUnit.SECONDS));
            long elapsedNanos = System.nanoTime() - requestedAt;
            assertTrue(elapsedNanos >= shutdownBudget.minusMillis(30).toNanos(),
                    "shutdown outcome 不应在共享预算前定稿");
            assertTrue(elapsedNanos < Duration.ofSeconds(1).toNanos(),
                    "startup rollback 卡住时也必须在共享预算附近返回");
            assertTrue(timeout.getCause() instanceof DisruptorShutdownException);
            assertTrue(timeout.getCause().getMessage().contains("超过共享关闭预算"));
            assertFalse(runtime.termination().toCompletableFuture().isDone(),
                    "超时 outcome 不能伪造 child 已真实终止");

            releaseThreadFactory.countDown();
            assertThrows(ExecutionException.class,
                    () -> start.toCompletableFuture().get(2, TimeUnit.SECONDS));
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            ExecutionException afterRollback = assertThrows(ExecutionException.class,
                    () -> runtime.haltAsync().toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertSame(timeout.getCause(), afterRollback.getCause(),
                    "迟到的 startup rollback 不能替换既有超时结果");
        } finally {
            releaseThreadFactory.countDown();
            runtime.haltAsync();
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void everyChildTerminationAndSnapshotFailureReachesTheShutdownOutcome() {
        IllegalStateException firstTerminationFailure =
                new IllegalStateException("first termination failed");
        IllegalStateException secondTerminationFailure =
                new IllegalStateException("second termination failed");
        IllegalStateException snapshotFailure = new IllegalStateException("pipeline failed");
        StubPipeline first = new StubPipeline("first-termination-failure");
        first.failTermination(firstTerminationFailure, null, ShutdownMode.GRACEFUL);
        StubPipeline second = new StubPipeline("second-termination-failure");
        second.failTermination(
                secondTerminationFailure, snapshotFailure, ShutdownMode.GRACEFUL);
        DisruptorRuntime runtime = new DisruptorRuntime(
                List.of(first, second), Duration.ofSeconds(2));

        CompletionException shutdownFailure = assertThrows(CompletionException.class,
                () -> runtime.shutdownAsync().toCompletableFuture().join());
        CompletionException runtimeTerminationFailure = assertThrows(CompletionException.class,
                () -> runtime.termination().toCompletableFuture().join());

        assertSuppressedIdentity(shutdownFailure.getCause(), firstTerminationFailure);
        assertSuppressedIdentity(shutdownFailure.getCause(), secondTerminationFailure);
        assertSuppressedIdentity(shutdownFailure.getCause(), snapshotFailure);
        assertSuppressedIdentity(runtimeTerminationFailure.getCause(), firstTerminationFailure);
        assertSuppressedIdentity(runtimeTerminationFailure.getCause(), secondTerminationFailure);
    }

    @Test
    void requestFailureIsAggregatedWithoutSkippingRemainingPipelines() {
        IllegalStateException requestFailure = new IllegalStateException("request failed");
        StubPipeline failing = new StubPipeline("request-failure")
                .failRequest(requestFailure);
        failing.completeTermination(ShutdownMode.GRACEFUL, null);
        StubPipeline healthy = new StubPipeline("request-healthy");
        DisruptorRuntime runtime = new DisruptorRuntime(
                List.of(failing, healthy), Duration.ofSeconds(2));

        CompletionStage<Void> firstOutcome = runtime.shutdownAsync();
        CompletionStage<Void> repeatedOutcome = runtime.shutdownAsync();
        healthy.completeTermination(ShutdownMode.GRACEFUL, null);
        CompletionException shutdownFailure = assertThrows(CompletionException.class,
                () -> firstOutcome.toCompletableFuture().join());

        assertSame(firstOutcome, repeatedOutcome);
        assertEquals(2, healthy.deadlines.size(), "单个 child 请求失败不能跳过后续 child");
        assertSuppressedIdentity(shutdownFailure.getCause(), requestFailure);
        assertEquals(1L, java.util.Arrays.stream(shutdownFailure.getCause().getSuppressed())
                .filter(failure -> failure == requestFailure)
                .count(), "重复广播的同一异常对象只能聚合一次");
    }

    @Test
    void shutdownOutcomeWaitsForEveryConcurrentBroadcastToFinish() throws Throwable {
        IllegalStateException requestFailure = new IllegalStateException("late request failed");
        CountDownLatch secondRequestEntered = new CountDownLatch(1);
        CountDownLatch releaseSecondRequest = new CountDownLatch(1);
        StubPipeline pipeline = new StubPipeline("in-flight-broadcast")
                .blockAndFailRequest(
                        2, secondRequestEntered, releaseSecondRequest, requestFailure);
        DisruptorRuntime runtime = new DisruptorRuntime(
                List.of(pipeline), Duration.ofSeconds(2));
        CompletionStage<Void> outcome = runtime.shutdownAsync();
        CountDownLatch outcomeCompleted = new CountDownLatch(1);
        outcome.whenComplete((ignored, failure) -> outcomeCompleted.countDown());
        AtomicReference<CompletionStage<Void>> repeatedOutcome = new AtomicReference<>();
        Thread haltCaller = Thread.ofPlatform().start(
                () -> repeatedOutcome.set(runtime.haltAsync()));
        Throwable testFailure = null;
        try {
            assertTrue(secondRequestEntered.await(2, TimeUnit.SECONDS));
            pipeline.completeTermination(ShutdownMode.IMMEDIATE, null);

            assertFalse(outcome.toCompletableFuture().isDone());
            assertFalse(outcomeCompleted.await(100, TimeUnit.MILLISECONDS),
                    "仍有同步广播在途时不能定稿 shutdown outcome");
            assertFalse(runtime.termination().toCompletableFuture().isDone());
            releaseSecondRequest.countDown();
            haltCaller.join(2_000);

            assertFalse(haltCaller.isAlive());
            assertSame(outcome, repeatedOutcome.get());
            CompletionException failure = assertThrows(CompletionException.class,
                    () -> outcome.toCompletableFuture().join());
            assertSuppressedIdentity(failure.getCause(), requestFailure);
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertSame(outcome, runtime.shutdownAsync());
            assertSame(outcome, runtime.haltAsync());
            assertEquals(2, pipeline.requestCount,
                    "outcome 承诺定稿后不能再接受新的 shutdown 广播");
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            try {
                releaseSecondRequest.countDown();
                pipeline.completeTermination(ShutdownMode.IMMEDIATE, null);
                haltCaller.join(2_000);
                runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            } catch (Throwable cleanupFailure) {
                if (testFailure == null) {
                    throw cleanupFailure;
                }
                testFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    @Test
    void shutdownAndImmediateUpgradeAreBroadcastBeforeTheirApiCallsReturn() throws Exception {
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ofSeconds(2))
                .add(PipelineSpec.builder("api-boundary", TestEvent.class,
                                () -> new TestEvent("slot"))
                        .topology(disruptor -> disruptor.handleEventsWith(
                                (event, sequence, endOfBatch) -> {
                                    handlerEntered.countDown();
                                    awaitUninterruptibly(releaseHandler);
                                }))
                        .build())
                .build();
        runtime.start();
        try {
            assertEquals(PublicationResult.PUBLISHED,
                    runtime.require("api-boundary", TestEvent.class)
                            .publishEvent(TRANSLATOR, "blocked", 1L, PUBLISH_TIMEOUT));
            assertTrue(handlerEntered.await(2, TimeUnit.SECONDS));
            Object pipelineLock = lifecycleLock(
                    runtime.requirePipeline("api-boundary", TestEvent.class));
            AtomicReference<CompletionStage<Void>> gracefulResult = new AtomicReference<>();
            AtomicBoolean gracefulReturned = new AtomicBoolean();
            Thread gracefulCaller;
            synchronized (pipelineLock) {
                gracefulCaller = Thread.ofPlatform().start(() -> {
                    gracefulResult.set(runtime.shutdownAsync());
                    gracefulReturned.set(true);
                });
                awaitCondition(() -> gracefulCaller.getState() == Thread.State.BLOCKED,
                        Duration.ofSeconds(2));
                assertFalse(gracefulReturned.get(),
                        "shutdownAsync 返回前必须同步广播 GRACEFUL");
            }
            gracefulCaller.join(2_000);
            assertTrue(gracefulReturned.get());
            assertEquals(ShutdownMode.GRACEFUL,
                    runtime.requirePipeline(
                            "api-boundary", TestEvent.class).snapshot().shutdownMode());

            AtomicReference<CompletionStage<Void>> repeatedGracefulResult = new AtomicReference<>();
            AtomicBoolean repeatedGracefulReturned = new AtomicBoolean();
            Thread repeatedGracefulCaller;
            synchronized (pipelineLock) {
                repeatedGracefulCaller = Thread.ofPlatform().start(() -> {
                    repeatedGracefulResult.set(runtime.shutdownAsync());
                    repeatedGracefulReturned.set(true);
                });
                awaitCondition(() -> repeatedGracefulCaller.getState() == Thread.State.BLOCKED,
                        Duration.ofSeconds(2));
                assertFalse(repeatedGracefulReturned.get(),
                        "重复 shutdownAsync 返回前也必须同步广播 GRACEFUL");
            }
            repeatedGracefulCaller.join(2_000);
            assertTrue(repeatedGracefulReturned.get());
            assertSame(gracefulResult.get(), repeatedGracefulResult.get());

            AtomicReference<CompletionStage<Void>> immediateResult = new AtomicReference<>();
            AtomicBoolean immediateReturned = new AtomicBoolean();
            Thread immediateCaller;
            synchronized (pipelineLock) {
                immediateCaller = Thread.ofPlatform().start(() -> {
                    immediateResult.set(runtime.haltAsync());
                    immediateReturned.set(true);
                });
                awaitCondition(() -> immediateCaller.getState() == Thread.State.BLOCKED,
                        Duration.ofSeconds(2));
                assertFalse(immediateReturned.get(),
                        "haltAsync 返回前必须同步广播 IMMEDIATE 升级");
            }
            immediateCaller.join(2_000);
            assertTrue(immediateReturned.get());
            assertEquals(ShutdownMode.IMMEDIATE,
                    runtime.requirePipeline(
                            "api-boundary", TestEvent.class).snapshot().shutdownMode());
            assertSame(gracefulResult.get(), immediateResult.get());

            releaseHandler.countDown();
            gracefulResult.get().toCompletableFuture().get(2, TimeUnit.SECONDS);
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        } finally {
            releaseHandler.countDown();
            runtime.haltAsync();
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
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
        assertEquals(started.getCount(), stopped.getCount(),
                "worker 若进入 LMAX runnable，onStart/onShutdown 必须成对；未入场则二者都不执行");
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

        assertThrows(NullPointerException.class, () -> handle.publish(null, PUBLISH_TIMEOUT));
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
        }, PUBLISH_TIMEOUT));
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
            assertEquals(PublicationResult.PUBLISHED,
                    runtime.require("slow", TestEvent.class).publishEvent(
                            TRANSLATOR, "x", 1L, PUBLISH_TIMEOUT));
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
        assertEquals(2, failingProcessor.haltCalls.get(),
                "graceful halt 失败后应再应用一次 immediate halt，两个阶段各至多一次");
        assertFalse(runtime.isRunning());
    }

    @RepeatedTest(20)
    void drainsInflightEventsAfterDelayedConsumerEntry() throws Exception {
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

        var start = runtime.startAsync().toCompletableFuture();
        assertTrue(consumerThreadCreated.await(2, TimeUnit.SECONDS));
        assertFalse(start.isDone(), "全部 consumer 实际入场前启动不能完成");
        allowConsumerToRun.countDown();
        start.get(2, TimeUnit.SECONDS);
        PipelineHandle<TestEvent> handle = runtime.require("drain", TestEvent.class);
        for (int i = 0; i < total; i++) {
            assertEquals(PublicationResult.PUBLISHED,
                    handle.publishEvent(TRANSLATOR, "v", (long) i, PUBLISH_TIMEOUT));
        }

        Thread shutdown = Thread.ofPlatform().name("delayed-consumer-shutdown").start(() -> {
            try {
                runtime.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        });
        awaitCondition(() -> !runtime.isRunning(), Duration.ofSeconds(2));
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
        Thread publisher = null;
        Thread shutdown = null;
        Throwable testFailure = null;
        try {
            runtime.start();
            publisher = Thread.ofPlatform().name("test-publisher").start(() -> {
                try {
                    PublicationResult result = handle.publishEvent((event, sequence) -> {
                        translating.countDown();
                        try {
                            releaseTranslator.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("发布线程被中断", interrupted);
                        }
                        event.value = "accepted";
                    }, PUBLISH_TIMEOUT);
                    if (result != PublicationResult.PUBLISHED) {
                        throw new AssertionError("发布失败：" + result);
                    }
                } catch (Throwable failure) {
                    publisherFailure.set(failure);
                }
            });
            assertTrue(translating.await(2, TimeUnit.SECONDS));

            shutdown = Thread.ofPlatform().name("test-shutdown").start(() -> {
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
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            try {
                releaseTranslator.countDown();
                runtime.haltAsync();
                interruptAndJoin(publisher);
                interruptAndJoin(shutdown);
                runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            } catch (Throwable cleanupFailure) {
                if (testFailure == null) {
                    throw cleanupFailure;
                }
                testFailure.addSuppressed(cleanupFailure);
            }
        }
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
        Thread[] publishers = new Thread[publisherCount];
        Thread shutdown = null;
        Throwable testFailure = null;
        try {
            runtime.start();
            for (int index = 0; index < publisherCount; index++) {
                publishers[index] = Thread.ofPlatform().name("multi-publisher-" + index).start(() -> {
                    try {
                        PublicationResult result = handle.publishEvent((event, sequence) -> {
                            translating.countDown();
                            try {
                                releaseTranslators.await();
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("发布线程被中断", interrupted);
                            }
                            event.number = sequence;
                        }, PUBLISH_TIMEOUT);
                        if (result != PublicationResult.PUBLISHED) {
                            throw new AssertionError("发布失败：" + result);
                        }
                    } catch (Throwable failure) {
                        publisherFailure.compareAndSet(null, failure);
                    }
                });
            }
            assertTrue(translating.await(2, TimeUnit.SECONDS));

            shutdown = Thread.ofPlatform().name("multi-publisher-shutdown").start(() -> {
                try {
                    runtime.shutdown();
                } catch (Throwable failure) {
                    shutdownFailure.set(failure);
                }
            });
            awaitCondition(() -> handle.tryPublishEvent(TRANSLATOR, "probe", 1L)
                            == PublicationResult.NOT_RUNNING,
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
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            try {
                releaseTranslators.countDown();
                runtime.haltAsync();
                for (Thread publisher : publishers) {
                    interruptAndJoin(publisher);
                }
                interruptAndJoin(shutdown);
                runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            } catch (Throwable cleanupFailure) {
                if (testFailure == null) {
                    throw cleanupFailure;
                }
                testFailure.addSuppressed(cleanupFailure);
            }
        }
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
        assertEquals(PublicationResult.PUBLISHED,
                secondHandle.publishEvent(
                        TRANSLATOR, "blocking", 1L, PUBLISH_TIMEOUT));
        assertTrue(secondHandlerEntered.await(2, TimeUnit.SECONDS));

        Thread shutdown = Thread.ofPlatform().name("multi-pipeline-shutdown").start(() -> {
            try {
                runtime.shutdown();
            } catch (Throwable failure) {
                shutdownFailure.set(failure);
            }
        });
        awaitCondition(() -> secondHandle.tryPublishEvent(TRANSLATOR, "probe", 2L)
                        == PublicationResult.NOT_RUNNING,
                Duration.ofSeconds(2));

        assertEquals(PublicationResult.NOT_RUNNING,
                firstHandle.tryPublishEvent(TRANSLATOR, "late", 3L),
                "排空任一管道前必须先关闭全部受管发布入口");

        releaseSecondHandler.countDown();
        shutdown.join(2_000);

        assertFalse(shutdown.isAlive());
        assertEquals(null, shutdownFailure.get());
    }

    @Test
    void rejectsManagedPublishingOutsideRunningState() throws Exception {
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec("managed-state")).build();
        PipelineHandle<TestEvent> handle = runtime.require("managed-state", TestEvent.class);

        assertEquals(PublicationResult.NOT_RUNNING,
                handle.tryPublishEvent(TRANSLATOR, "before", 1L));
        assertEquals(PublicationResult.NOT_RUNNING,
                handle.publishEvent(TRANSLATOR, "before", 1L, PUBLISH_TIMEOUT));

        runtime.start();
        assertEquals(PublicationResult.PUBLISHED,
                handle.tryPublishEvent(TRANSLATOR, "running", 2L));
        runtime.shutdown();

        assertEquals(PublicationResult.NOT_RUNNING,
                handle.tryPublishEvent(TRANSLATOR, "after", 3L));
        assertEquals(PublicationResult.NOT_RUNNING,
                handle.publishEvent(TRANSLATOR, "after", 3L, PUBLISH_TIMEOUT));
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
        assertEquals(PublicationResult.PUBLISHED,
                handle.publishEvent(zero, PUBLISH_TIMEOUT));
        assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(one, 1L));
        assertEquals(PublicationResult.PUBLISHED,
                handle.publishEvent(TRANSLATOR, "two", 2L, PUBLISH_TIMEOUT));
        assertEquals(PublicationResult.PUBLISHED,
                handle.tryPublishEvent(three, "three", 3L, true));

        assertTrue(consumed.await(2, TimeUnit.SECONDS));
        runtime.shutdown();
    }

    @Test
    void asynchronousStartCompletesOnlyAfterEveryPipelineWorkerEntered() throws Exception {
        CountDownLatch delayedThreadCreated = new CountDownLatch(1);
        CountDownLatch allowDelayedEntry = new CountDownLatch(1);
        PipelineSpec<TestEvent> delayed = PipelineSpec.builder(
                        "delayed", TestEvent.class, () -> new TestEvent("slot"))
                .threadFactory(runnable -> new Thread(() -> {
                    delayedThreadCreated.countDown();
                    awaitUninterruptibly(allowDelayedEntry);
                    runnable.run();
                }, "runtime-delayed-worker"))
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> { }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .add(spec("first"))
                .add(delayed)
                .build();

        var start = runtime.startAsync().toCompletableFuture();

        assertTrue(delayedThreadCreated.await(2, TimeUnit.SECONDS));
        assertFalse(start.isDone());
        allowDelayedEntry.countDown();
        start.get(2, TimeUnit.SECONDS);

        assertTrue(runtime.isRunning());
        assertEquals(List.of(PipelineHealth.HEALTHY, PipelineHealth.HEALTHY),
                runtime.handles().stream().map(PipelineHandle::snapshot)
                        .map(PipelineSnapshot::health).toList());
        runtime.shutdown();
    }

    @Test
    void consumerFailureStopsOnlyItsOwningPipeline() throws Exception {
        CountDownLatch failedHandlerEntered = new CountDownLatch(1);
        PipelineSpec<TestEvent> failing = PipelineSpec.builder(
                        "failing", TestEvent.class, () -> new TestEvent("slot"))
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> {
                            failedHandlerEntered.countDown();
                            throw new IllegalStateException("pipeline failed");
                        }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .add(failing)
                .add(spec("healthy"))
                .build();
        PipelineHandle<TestEvent> failingHandle = runtime.require("failing", TestEvent.class);
        PipelineHandle<TestEvent> healthyHandle = runtime.require("healthy", TestEvent.class);
        runtime.start();

        assertEquals(PublicationResult.PUBLISHED,
                failingHandle.publishEvent(TRANSLATOR, "fail", 1L, PUBLISH_TIMEOUT));

        assertTrue(failedHandlerEntered.await(2, TimeUnit.SECONDS));
        awaitCondition(() -> failingHandle.snapshot().lifecycle() == SupervisedLifecycle.TERMINATED,
                Duration.ofSeconds(2));
        assertEquals(PipelineHealth.TERMINATED, failingHandle.snapshot().health());
        assertNotNull(failingHandle.snapshot().failure());
        assertEquals(PipelineHealth.HEALTHY, healthyHandle.snapshot().health());
        assertEquals(PublicationResult.PUBLISHED,
                healthyHandle.tryPublishEvent(TRANSLATOR, "still-running", 2L));

        assertThrows(DisruptorShutdownException.class, runtime::halt,
                "已锁存的 consumer 基础设施故障必须进入 Runtime 聚合结果");
    }

    @Test
    void shutdownSharesOneDeadlineAndTerminationWaitsForActualWorkerExit() throws Exception {
        int pipelineCount = 3;
        CountDownLatch handlersEntered = new CountDownLatch(pipelineCount);
        CountDownLatch releaseHandlers = new CountDownLatch(1);
        DisruptorRuntime.Builder builder = DisruptorRuntime.builder()
                .shutdownTimeout(Duration.ofMillis(150));
        for (int index = 0; index < pipelineCount; index++) {
            String name = "blocked-" + index;
            builder.add(PipelineSpec.builder(name, TestEvent.class, () -> new TestEvent("slot"))
                    .topology(disruptor -> disruptor.handleEventsWith(
                            (event, sequence, endOfBatch) -> {
                                handlersEntered.countDown();
                                awaitUninterruptibly(releaseHandlers);
                            }))
                    .build());
        }
        DisruptorRuntime runtime = builder.build();
        runtime.start();
        try {
            for (PipelineHandle<?> rawHandle : runtime.handles()) {
                @SuppressWarnings("unchecked")
                PipelineHandle<TestEvent> handle = (PipelineHandle<TestEvent>) rawHandle;
                assertEquals(PublicationResult.PUBLISHED,
                        handle.publishEvent(
                                TRANSLATOR, "blocked", 1L, PUBLISH_TIMEOUT));
            }
            assertTrue(handlersEntered.await(2, TimeUnit.SECONDS));

            long startedAt = System.nanoTime();
            assertThrows(DisruptorShutdownException.class, runtime::shutdown);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

            assertTrue(elapsed.compareTo(Duration.ofMillis(500)) < 0,
                    "关闭预算必须是全局一次性的，不能按 child 线性叠加，实际=" + elapsed);
            assertFalse(runtime.termination().toCompletableFuture().isDone());
            assertEquals(Set.of(ShutdownMode.IMMEDIATE), runtime.handles().stream()
                    .map(PipelineHandle::snapshot).map(PipelineSnapshot::shutdownMode).collect(
                            java.util.stream.Collectors.toSet()));
            Set<String> deadlines = runtime.handles().stream()
                    .map(PipelineHandle::snapshot)
                    .map(PipelineSnapshot::failure)
                    .map(Throwable::getMessage)
                    .map(DisruptorRuntimeTest::deadlineFrom)
                    .collect(java.util.stream.Collectors.toSet());
            assertEquals(1, deadlines.size(), "所有 child 必须冻结同一个绝对 deadline");

            releaseHandlers.countDown();
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(Set.of(SupervisedLifecycle.TERMINATED), runtime.handles().stream()
                    .map(PipelineHandle::snapshot).map(PipelineSnapshot::lifecycle).collect(
                            java.util.stream.Collectors.toSet()));
        } finally {
            releaseHandlers.countDown();
            runtime.haltAsync();
            runtime.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
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

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void interruptAndJoin(Thread thread) throws InterruptedException {
        if (thread == null) {
            return;
        }
        thread.interrupt();
        thread.join(2_000);
        if (thread.isAlive()) {
            throw new AssertionError("清理线程超时：" + thread.getName());
        }
    }

    private static String deadlineFrom(String message) {
        int start = message.indexOf("deadlineNanos=");
        int end = message.indexOf('，', start);
        return message.substring(start, end);
    }

    private static Object lifecycleLock(DisruptorPipeline<?> pipeline) {
        try {
            var field = pipeline.getClass().getDeclaredField("lifecycleLock");
            field.setAccessible(true);
            return field.get(pipeline);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("无法读取 ManagedPipeline 生命周期锁", failure);
        }
    }

    private static void assertSuppressedIdentity(Throwable aggregate, Throwable expected) {
        assertTrue(java.util.Arrays.stream(aggregate.getSuppressed())
                .anyMatch(failure -> failure == expected));
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

    private static final class StubPipeline
            implements DisruptorPipeline<TestEvent>, PipelineHandle<TestEvent> {

        private final String name;
        private final RingBuffer<TestEvent> ringBuffer = RingBuffer.createSingleProducer(
                () -> new TestEvent("stub"), 2);
        private final CompletableFuture<Void> startOutcome = new CompletableFuture<>();
        private final CompletableFuture<PipelineSnapshot> terminationOutcome =
                new CompletableFuture<>();
        private final List<ShutdownDeadline> deadlines = new CopyOnWriteArrayList<>();
        private Throwable startFailure;
        private Throwable requestFailure;
        private int blockedRequestNumber;
        private int requestCount;
        private CountDownLatch blockedRequestEntered;
        private CountDownLatch releaseBlockedRequest;
        private boolean completeOnRequest;
        private volatile PipelineSnapshot snapshot;

        private StubPipeline(String name) {
            this.name = name;
            this.snapshot = snapshot(name, SupervisedLifecycle.NEW, null, null);
            startOutcome.complete(null);
        }

        private StubPipeline failStart(Throwable failure) {
            startFailure = failure;
            startOutcome.obtrudeException(failure);
            return this;
        }

        private StubPipeline failRequest(Throwable failure) {
            requestFailure = failure;
            return this;
        }

        private StubPipeline terminateOnRequest() {
            completeOnRequest = true;
            return this;
        }

        private StubPipeline blockAndFailRequest(
                int requestNumber,
                CountDownLatch entered,
                CountDownLatch release,
                Throwable failure) {
            blockedRequestNumber = requestNumber;
            blockedRequestEntered = entered;
            releaseBlockedRequest = release;
            requestFailure = failure;
            return this;
        }

        private void failTermination(
                Throwable terminationFailure,
                Throwable snapshotFailure,
                ShutdownMode mode) {
            snapshot = snapshot(name, SupervisedLifecycle.TERMINATED, snapshotFailure, mode);
            terminationOutcome.completeExceptionally(terminationFailure);
        }

        private void completeTermination(ShutdownMode mode, Throwable failure) {
            snapshot = snapshot(name, SupervisedLifecycle.TERMINATED, failure, mode);
            terminationOutcome.complete(snapshot);
        }

        @Override
        public PipelineHandle<TestEvent> handle() {
            return this;
        }

        @Override
        public PipelineSnapshot snapshot() {
            return snapshot;
        }

        @Override
        public CompletionStage<Void> start() {
            return startOutcome;
        }

        @Override
        public synchronized void requestShutdown(
                ShutdownMode mode,
                ShutdownDeadline deadline) {
            deadlines.add(deadline);
            requestCount++;
            if (completeOnRequest && !terminationOutcome.isDone()) {
                completeTermination(mode, startFailure);
            }
            if (requestCount == blockedRequestNumber) {
                blockedRequestEntered.countDown();
                awaitUninterruptibly(releaseBlockedRequest);
            }
            if (requestFailure != null
                    && (blockedRequestNumber == 0 || requestCount == blockedRequestNumber)) {
                sneakyThrow(requestFailure);
            }
        }

        @Override
        public CompletionStage<PipelineSnapshot> termination() {
            return terminationOutcome;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<TestEvent> eventType() {
            return TestEvent.class;
        }

        @Override
        public PublicationResult tryPublishEvent(EventTranslator<TestEvent> translator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PublicationResult publishEvent(
                EventTranslator<TestEvent> translator,
                Duration timeout) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A> PublicationResult tryPublishEvent(
                EventTranslatorOneArg<TestEvent, A> translator,
                A arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A> PublicationResult publishEvent(
                EventTranslatorOneArg<TestEvent, A> translator,
                A arg0,
                Duration timeout) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A, B> PublicationResult tryPublishEvent(
                EventTranslatorTwoArg<TestEvent, A, B> translator,
                A arg0,
                B arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A, B> PublicationResult publishEvent(
                EventTranslatorTwoArg<TestEvent, A, B> translator,
                A arg0,
                B arg1,
                Duration timeout) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A, B, C> PublicationResult tryPublishEvent(
                EventTranslatorThreeArg<TestEvent, A, B, C> translator,
                A arg0,
                B arg1,
                C arg2) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A, B, C> PublicationResult publishEvent(
                EventTranslatorThreeArg<TestEvent, A, B, C> translator,
                A arg0,
                B arg1,
                C arg2,
                Duration timeout) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public RingBuffer<TestEvent> unsafeRingBuffer() {
            return ringBuffer;
        }

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
            throw (T) failure;
        }

        private static PipelineSnapshot snapshot(
                String name,
                SupervisedLifecycle lifecycle,
                Throwable failure,
                ShutdownMode mode) {
            boolean terminated = lifecycle == SupervisedLifecycle.TERMINATED;
            return PipelineSnapshot.builder()
                    .name(name)
                    .lifecycle(lifecycle)
                    .acceptingPublications(false)
                    .registrationSealed(terminated)
                    .expectedConsumers(0)
                    .createdConsumers(0)
                    .startedConsumers(0)
                    .aliveConsumers(0)
                    .bufferSize(0)
                    .backlog(0L)
                    .failure(failure)
                    .shutdownMode(mode)
                    .reachedRunning(false)
                    .drainCommitted(false)
                    .gracefulStopApplied(false)
                    .build();
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
            running.set(false);
            throw new IllegalStateException("halt failed");
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void run() {
            running.set(true);
            while (running.get()) {
                LockSupport.parkNanos(100_000L);
            }
        }
    }
}
