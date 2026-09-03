package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisruptorPipelineTest {

    @Test
    void startsOnlyAfterEveryDynamicallyRegisteredWorkerHasEntered() throws Exception {
        CountDownLatch threadCreated = new CountDownLatch(1);
        CountDownLatch allowEntry = new CountDownLatch(1);
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "delayed", TestEvent.class, TestEvent::new)
                .threadFactory(runnable -> new Thread(() -> {
                    threadCreated.countDown();
                    awaitUninterruptibly(allowEntry);
                    runnable.run();
                }, "delayed-worker"))
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> { }))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);

        var start = pipeline.start().toCompletableFuture();

        assertTrue(threadCreated.await(2, TimeUnit.SECONDS));
        assertFalse(start.isDone());
        PipelineSnapshot starting = pipeline.snapshot();
        assertEquals(PipelineLifecycle.STARTING, starting.lifecycle());
        assertTrue(starting.registrationSealed());
        assertEquals(1, starting.expectedConsumers());
        assertEquals(1, starting.createdConsumers());
        assertEquals(0, starting.startedConsumers());

        allowEntry.countDown();
        start.get(2, TimeUnit.SECONDS);

        PipelineSnapshot running = pipeline.snapshot();
        assertEquals(PipelineLifecycle.RUNNING, running.lifecycle());
        assertEquals(PipelineHealth.HEALTHY, running.health());
        assertEquals(1, running.startedConsumers());
        assertEquals(1, running.aliveConsumers());
        stop(pipeline);
    }

    @Test
    void sealsAllStartAttemptsAndPreservesTheOriginalStartupFailure() throws Exception {
        IllegalStateException expected = new IllegalStateException("second worker rejected");
        AtomicInteger created = new AtomicInteger();
        AtomicReference<Thread> firstThread = new AtomicReference<>();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "partial", TestEvent.class, TestEvent::new)
                .threadFactory(runnable -> {
                    if (created.incrementAndGet() == 2) {
                        throw expected;
                    }
                    Thread thread = new Thread(runnable, "partial-worker");
                    firstThread.set(thread);
                    return thread;
                })
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> { },
                        (event, sequence, endOfBatch) -> { }))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);

        CompletionException failure = assertThrows(CompletionException.class,
                () -> pipeline.start().toCompletableFuture().join());

        assertSame(expected, failure.getCause());
        PipelineSnapshot snapshot = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        firstThread.get().join(2_000);
        assertFalse(firstThread.get().isAlive());
        assertTrue(snapshot.registrationSealed());
        assertEquals(1, snapshot.expectedConsumers());
        assertEquals(1, snapshot.createdConsumers());
        assertSame(expected, snapshot.failure());
        assertEquals(ShutdownMode.IMMEDIATE, snapshot.shutdownMode());
    }

    @Test
    void doesNotCaptureDrainCursorUntilEveryAdmittedPublisherReturns() throws Exception {
        ObservingProcessor processor = new ObservingProcessor();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "publisher-boundary", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(processor))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        CountDownLatch translating = new CountDownLatch(1);
        CountDownLatch releaseTranslator = new CountDownLatch(1);
        Thread publisher = Thread.ofPlatform().start(() -> pipeline.handle().publishEvent(
                (event, sequence) -> {
                    translating.countDown();
                    awaitUninterruptibly(releaseTranslator);
                }));
        assertTrue(translating.await(2, TimeUnit.SECONDS));

        processor.sequence.observe();
        pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofSeconds(2)));

        assertFalse(processor.sequence.awaitRead(Duration.ofMillis(100)),
                "在途 publisher 返回前不得捕获 drain cursor 或读取叶子 gating sequence");
        releaseTranslator.countDown();
        assertTrue(processor.sequence.awaitRead(Duration.ofSeconds(2)));
        publisher.join(2_000);
        processor.sequence.set(0L);

        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertTrue(terminated.gracefulTermination());
    }

    @Test
    void capturesOneFixedCursorAndWaitsForEveryLeafGatingSequence() throws Exception {
        ObservingProcessor first = new ObservingProcessor();
        ObservingProcessor second = new ObservingProcessor();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "leaf-gates", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(first, second))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        pipeline.handle().unsafeRingBuffer().publishEvent((event, sequence) -> { });

        first.sequence.observe();
        second.sequence.observe();
        pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofSeconds(2)));
        assertTrue(first.sequence.awaitRead(Duration.ofSeconds(2)));
        assertTrue(second.sequence.awaitRead(Duration.ofSeconds(2)));

        pipeline.handle().unsafeRingBuffer().publishEvent((event, sequence) -> { });
        first.sequence.set(0L);
        assertFalse(pipeline.termination().toCompletableFuture().isDone(),
                "任一叶子未到固定目标前不能提交排空");
        second.sequence.set(0L);

        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertTrue(terminated.gracefulTermination(),
                "目标固定后不应追逐绕过托管入口的新 cursor");
        assertEquals(1L, pipeline.handle().unsafeRingBuffer().getCursor());
    }

    @Test
    void invokesHaltOnlyAfterThePipelineEnteredStopping() throws Exception {
        AtomicReference<DisruptorPipeline<TestEvent>> pipelineRef = new AtomicReference<>();
        AtomicReference<PipelineLifecycle> lifecycleAtHalt = new AtomicReference<>();
        ObservingProcessor processor = new ObservingProcessor() {
            @Override
            public void halt() {
                lifecycleAtHalt.set(pipelineRef.get().snapshot().lifecycle());
                super.halt();
            }
        };
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "halt-order", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(processor))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);
        pipelineRef.set(pipeline);
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);

        pipeline.requestShutdown(ShutdownMode.IMMEDIATE,
                ShutdownDeadline.after(Duration.ofSeconds(2)));
        pipeline.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertEquals(PipelineLifecycle.STOPPING, lifecycleAtHalt.get());
        assertEquals(1, processor.haltCalls.get());
    }

    @Test
    void immediateStopOfHealthyLmaxWorkerDoesNotInventAFailure() throws Exception {
        DisruptorPipeline<TestEvent> pipeline = pipeline(PipelineSpec.builder(
                        "healthy-immediate", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> { }))
                .build());
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);

        pipeline.requestShutdown(ShutdownMode.IMMEDIATE,
                ShutdownDeadline.after(Duration.ofSeconds(2)));
        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertEquals(PipelineLifecycle.TERMINATED, terminated.lifecycle());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertNull(terminated.failure(), "受控中断不能被误报成 consumer 故障");
        assertFalse(terminated.gracefulTermination());
    }

    @Test
    void completedStartWinningBeforeShutdownClosesTheGateWithoutFailure() throws Exception {
        DisruptorPipeline<TestEvent> pipeline = pipeline(PipelineSpec.builder(
                        "start-wins-close", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> { }))
                .build());
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(pipeline.handle().tryPublishEvent((event, sequence) -> { }));

        pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofSeconds(2)));

        assertFalse(pipeline.handle().tryPublishEvent((event, sequence) -> { }));
        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertNull(terminated.failure());
        assertTrue(terminated.gracefulTermination());
    }

    @Test
    void shutdownBeforeStartTerminatesAndEveryStartReturnsTheSameFailedStage() throws Exception {
        DisruptorPipeline<TestEvent> pipeline = pipeline(PipelineSpec.builder(
                        "never-started", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> { }))
                .build());

        pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofSeconds(2)));
        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        CompletionStage<Void> firstStart = pipeline.start();
        CompletionStage<Void> repeatedStart = pipeline.start();

        assertEquals(PipelineLifecycle.TERMINATED, terminated.lifecycle());
        assertSame(firstStart, repeatedStart);
        assertTrue(firstStart.toCompletableFuture().isCompletedExceptionally());
        assertThrows(CompletionException.class, () -> firstStart.toCompletableFuture().join());
        assertFalse(pipeline.handle().tryPublishEvent((event, sequence) -> { }));
    }

    @Test
    void gracefulShutdownWinningDuringStartPermanentlyClosesPublicationGate() throws Exception {
        CountDownLatch threadCreated = new CountDownLatch(1);
        CountDownLatch allowEntry = new CountDownLatch(1);
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "close-wins-start", TestEvent.class, TestEvent::new)
                .threadFactory(runnable -> new Thread(() -> {
                    threadCreated.countDown();
                    awaitUninterruptibly(allowEntry);
                    runnable.run();
                }, "close-wins-worker"))
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> { }))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);
        var start = pipeline.start().toCompletableFuture();
        assertTrue(threadCreated.await(2, TimeUnit.SECONDS));

        pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofSeconds(2)));
        assertEquals(PipelineLifecycle.STOPPING, pipeline.snapshot().lifecycle());
        assertFalse(pipeline.handle().tryPublishEvent((event, sequence) -> { }));
        allowEntry.countDown();

        assertThrows(Exception.class, () -> start.get(2, TimeUnit.SECONDS));
        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertNull(terminated.failure(), "正常并发关闭不能伪造基础设施故障");
        assertFalse(pipeline.handle().tryPublishEvent((event, sequence) -> { }));
    }

    @Test
    void genuineThrowableWithInterruptedCauseIsNotSwallowedDuringImmediateStop() throws Exception {
        RuntimeException original = new RuntimeException(
                "genuine consumer failure", new InterruptedException("business cause"));
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        EventProcessor processor = new EventProcessor() {
            private final Sequence sequence = new Sequence();
            private final AtomicBoolean running = new AtomicBoolean();

            @Override
            public Sequence getSequence() {
                return sequence;
            }

            @Override
            public void halt() {
                releaseWorker.countDown();
            }

            @Override
            public boolean isRunning() {
                return running.get();
            }

            @Override
            public void run() {
                running.set(true);
                workerEntered.countDown();
                awaitUninterruptibly(releaseWorker);
                throw original;
            }
        };
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "preserve-throwable", TestEvent.class, TestEvent::new)
                .threadFactory(runnable -> {
                    Thread thread = new Thread(runnable, "preserve-throwable-worker");
                    thread.setUncaughtExceptionHandler((ignored, failure) -> uncaught.set(failure));
                    return thread;
                })
                .topology(disruptor -> disruptor.handleEventsWith(processor))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(workerEntered.await(2, TimeUnit.SECONDS));

        pipeline.requestShutdown(ShutdownMode.IMMEDIATE,
                ShutdownDeadline.after(Duration.ofSeconds(2)));
        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertSame(original, terminated.failure());
        assertSame(original, uncaught.get());
    }

    private static DisruptorPipeline<TestEvent> pipeline(PipelineSpec<TestEvent> spec) {
        return ManagedPipeline.build(spec, spec.resolve(PipelineSettings.defaults()),
                Duration.ofSeconds(2));
    }

    private static void stop(DisruptorPipeline<?> pipeline) throws Exception {
        pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofSeconds(2)));
        pipeline.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
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

    private static class ObservingProcessor implements EventProcessor {
        private final ObservingSequence sequence = new ObservingSequence();
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

    private static final class ObservingSequence extends Sequence {
        private final AtomicBoolean observing = new AtomicBoolean();
        private final CountDownLatch read = new CountDownLatch(1);

        @Override
        public long get() {
            if (observing.get()) {
                read.countDown();
            }
            return super.get();
        }

        private void observe() {
            observing.set(true);
        }

        private boolean awaitRead(Duration timeout) throws InterruptedException {
            return read.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private static final class TestEvent {
    }
}
