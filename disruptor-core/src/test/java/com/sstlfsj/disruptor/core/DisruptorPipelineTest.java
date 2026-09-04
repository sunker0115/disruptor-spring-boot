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
    void startupWorkerFailureClosesNewGateBeforeSupervisorSettlesAndPreservesCause()
            throws Throwable {
        IllegalStateException original = new IllegalStateException("startup worker failed");
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch failWorker = new CountDownLatch(1);
        CountDownLatch threadStarted = new CountDownLatch(1);
        CountDownLatch allowStartReturn = new CountDownLatch(1);
        CountDownLatch supervisorLockHeld = new CountDownLatch(1);
        CountDownLatch releaseSupervisorLock = new CountDownLatch(1);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        AtomicReference<Thread> supervisorLockHolder = new AtomicReference<>();
        AtomicReference<CompletionStage<Void>> startStage = new AtomicReference<>();
        AtomicReference<Throwable> startInvocationFailure = new AtomicReference<>();
        EventProcessor processor = new EventProcessor() {
            private final Sequence sequence = new Sequence();

            @Override
            public Sequence getSequence() {
                return sequence;
            }

            @Override
            public void halt() {
                failWorker.countDown();
            }

            @Override
            public boolean isRunning() {
                return workerEntered.getCount() == 0L && failWorker.getCount() != 0L;
            }

            @Override
            public void run() {
                workerEntered.countDown();
                awaitUninterruptibly(failWorker);
                throw original;
            }
        };
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "startup-worker-failure-window", TestEvent.class, TestEvent::new)
                .threadFactory(runnable -> {
                    Thread thread = new Thread(runnable, "startup-worker-failure-window") {
                        @Override
                        public synchronized void start() {
                            super.start();
                            threadStarted.countDown();
                            awaitUninterruptibly(allowStartReturn);
                        }
                    };
                    thread.setUncaughtExceptionHandler((ignored, failure) -> { });
                    workerThread.set(thread);
                    return thread;
                })
                .topology(disruptor -> disruptor.handleEventsWith(processor))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);
        Thread starter = Thread.ofPlatform().name("startup-worker-failure-starter").start(() -> {
            try {
                startStage.set(pipeline.start());
            } catch (Throwable failure) {
                startInvocationFailure.set(failure);
            }
        });
        Throwable testFailure = null;
        try {
            assertTrue(threadStarted.await(2, TimeUnit.SECONDS));
            assertTrue(workerEntered.await(2, TimeUnit.SECONDS));
            Object lifecycleLock = lifecycleLock(pipeline);
            Object supervisorStateLock = supervisorStateLock(pipeline);
            synchronized (lifecycleLock) {
                failWorker.countDown();
                awaitThreadState(workerThread.get(), Thread.State.BLOCKED);
                Thread lockHolder = Thread.ofPlatform()
                        .name("startup-worker-failure-supervisor-lock")
                        .start(() -> {
                            synchronized (supervisorStateLock) {
                                supervisorLockHeld.countDown();
                                awaitUninterruptibly(releaseSupervisorLock);
                            }
                        });
                supervisorLockHolder.set(lockHolder);
                assertTrue(supervisorLockHeld.await(2, TimeUnit.SECONDS));
            }

            awaitWorkerFailurePending(pipeline);
            assertEquals(PublicationResult.PIPELINE_FAILED,
                    pipeline.handle().tryPublishEvent((event, sequence) -> { }),
                    "supervisor 尚未锁存首因时，NEW gate 也必须明确报告管道故障");

            releaseSupervisorLock.countDown();
            allowStartReturn.countDown();
            starter.join(2_000);
            assertFalse(starter.isAlive());
            assertNull(startInvocationFailure.get());
            CompletionException startupFailure = assertThrows(CompletionException.class,
                    () -> startStage.get().toCompletableFuture().join());
            assertSame(original, startupFailure.getCause());
            PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                    .get(2, TimeUnit.SECONDS);
            assertSame(original, terminated.failure());
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            try {
                failWorker.countDown();
                releaseSupervisorLock.countDown();
                allowStartReturn.countDown();
                pipeline.requestShutdown(ShutdownMode.IMMEDIATE,
                        ShutdownDeadline.after(Duration.ofSeconds(2)));
                Thread lockHolder = supervisorLockHolder.get();
                interruptAndJoin(lockHolder);
                interruptAndJoin(starter);
                interruptAndJoin(workerThread.get());
                pipeline.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            } catch (Throwable cleanupFailure) {
                if (testFailure == null) {
                    throw cleanupFailure;
                }
                testFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    @Test
    void doesNotCaptureDrainCursorUntilEveryAdmittedPublisherReturns() throws Exception {
        ObservingProcessor processor = new ObservingProcessor();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "publisher-boundary", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(processor))
                .build();
        DisruptorPipeline<TestEvent> pipeline = pipeline(spec);
        CountDownLatch translating = new CountDownLatch(1);
        CountDownLatch releaseTranslator = new CountDownLatch(1);
        AtomicReference<Throwable> publicationFailure = new AtomicReference<>();
        Thread publisher = null;
        Throwable testFailure = null;
        try {
            pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
            publisher = Thread.ofPlatform().start(() -> {
                try {
                    assertEquals(PublicationResult.PUBLISHED, pipeline.handle().publishEvent(
                            (event, sequence) -> {
                                translating.countDown();
                                awaitUninterruptibly(releaseTranslator);
                            }, Duration.ofSeconds(2)));
                } catch (Throwable failure) {
                    publicationFailure.set(failure);
                }
            });
            assertTrue(translating.await(2, TimeUnit.SECONDS));

            processor.sequence.observe();
            pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                    ShutdownDeadline.after(Duration.ofSeconds(2)));

            assertFalse(processor.sequence.awaitRead(Duration.ofMillis(100)),
                    "在途 publisher 返回前不得捕获 drain cursor 或读取叶子 gating sequence");
            releaseTranslator.countDown();
            assertTrue(processor.sequence.awaitRead(Duration.ofSeconds(2)));
            publisher.join(2_000);
            assertFalse(publisher.isAlive());
            assertNull(publicationFailure.get());
            processor.sequence.set(0L);

            PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                    .get(2, TimeUnit.SECONDS);
            assertTrue(terminated.gracefulTermination());
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            try {
                releaseTranslator.countDown();
                pipeline.requestShutdown(ShutdownMode.IMMEDIATE,
                        ShutdownDeadline.after(Duration.ofSeconds(2)));
                interruptAndJoin(publisher);
                pipeline.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            } catch (Throwable cleanupFailure) {
                if (testFailure == null) {
                    throw cleanupFailure;
                }
                testFailure.addSuppressed(cleanupFailure);
            }
        }
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
        assertEquals(PublicationResult.PUBLISHED,
                pipeline.handle().tryPublishEvent((event, sequence) -> { }));

        pipeline.requestShutdown(ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofSeconds(2)));

        assertEquals(PublicationResult.NOT_RUNNING,
                pipeline.handle().tryPublishEvent((event, sequence) -> { }));
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
        assertEquals(PublicationResult.NOT_RUNNING,
                pipeline.handle().tryPublishEvent((event, sequence) -> { }));
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
        assertEquals(PublicationResult.NOT_RUNNING,
                pipeline.handle().tryPublishEvent((event, sequence) -> { }));
        allowEntry.countDown();

        assertThrows(Exception.class, () -> start.get(2, TimeUnit.SECONDS));
        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertNull(terminated.failure(), "正常并发关闭不能伪造基础设施故障");
        assertEquals(PublicationResult.NOT_RUNNING,
                pipeline.handle().tryPublishEvent((event, sequence) -> { }));
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

    @Test
    void observingStoppingAfterWorkerFailureMeansPublicationGateIsAlreadyClosed() throws Exception {
        RuntimeException original = new RuntimeException("worker failed");
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch failWorker = new CountDownLatch(1);
        CountDownLatch haltEntered = new CountDownLatch(1);
        CountDownLatch allowHaltReturn = new CountDownLatch(1);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        EventProcessor processor = new EventProcessor() {
            private final Sequence sequence = new Sequence();
            private final AtomicBoolean running = new AtomicBoolean();

            @Override
            public Sequence getSequence() {
                return sequence;
            }

            @Override
            public void halt() {
                haltEntered.countDown();
                awaitUninterruptibly(allowHaltReturn);
                running.set(false);
            }

            @Override
            public boolean isRunning() {
                return running.get();
            }

            @Override
            public void run() {
                running.set(true);
                workerEntered.countDown();
                awaitUninterruptibly(failWorker);
                throw original;
            }
        };
        DisruptorPipeline<TestEvent> pipeline = pipeline(PipelineSpec.builder(
                        "failure-closes-gate-first", TestEvent.class, TestEvent::new)
                .threadFactory(runnable -> {
                    Thread thread = new Thread(runnable, "failure-closes-gate-first-worker");
                    thread.setUncaughtExceptionHandler((ignored, failure) -> { });
                    workerThread.set(thread);
                    return thread;
                })
                .topology(disruptor -> disruptor.handleEventsWith(processor))
                .build());
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(workerEntered.await(2, TimeUnit.SECONDS));

        Object lifecycleLock = lifecycleLock(pipeline);
        try {
            boolean stoppingObservedWhileGateCloseWasBlocked;
            synchronized (lifecycleLock) {
                failWorker.countDown();
                long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
                while (workerThread.get().getState() != Thread.State.BLOCKED
                        && System.nanoTime() < deadline) {
                    LockSupport.parkNanos(100_000L);
                }
                assertEquals(Thread.State.BLOCKED, workerThread.get().getState());
                stoppingObservedWhileGateCloseWasBlocked =
                        pipeline.snapshot().lifecycle() == PipelineLifecycle.STOPPING;
                if (stoppingObservedWhileGateCloseWasBlocked) {
                    assertEquals(PublicationResult.PIPELINE_FAILED,
                            pipeline.handle().tryPublishEvent((event, sequence) -> { }),
                            "一旦可观察到 STOPPING，实际发布准入必须已经关闭");
                }
            }
            if (!stoppingObservedWhileGateCloseWasBlocked) {
                assertTrue(haltEntered.await(2, TimeUnit.SECONDS));
                assertEquals(PipelineLifecycle.STOPPING, pipeline.snapshot().lifecycle());
                assertEquals(PublicationResult.PIPELINE_FAILED,
                        pipeline.handle().tryPublishEvent((event, sequence) -> { }),
                        "一旦可观察到 STOPPING，实际发布准入必须已经关闭");
            }
        } finally {
            allowHaltReturn.countDown();
        }

        PipelineSnapshot terminated = pipeline.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertSame(original, terminated.failure());
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

    private static Object lifecycleLock(DisruptorPipeline<?> pipeline) {
        try {
            var field = pipeline.getClass().getDeclaredField("lifecycleLock");
            field.setAccessible(true);
            return field.get(pipeline);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("无法读取 ManagedPipeline 生命周期锁", failure);
        }
    }

    private static Object supervisorStateLock(DisruptorPipeline<?> pipeline) {
        try {
            var supervisorField = pipeline.getClass().getDeclaredField("supervisor");
            supervisorField.setAccessible(true);
            Object supervisor = supervisorField.get(pipeline);
            var stateLockField = supervisor.getClass().getDeclaredField("stateLock");
            stateLockField.setAccessible(true);
            return stateLockField.get(supervisor);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("无法读取 WorkerSupervisor 状态锁", failure);
        }
    }

    private static void awaitWorkerFailurePending(DisruptorPipeline<?> pipeline) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (!workerFailurePending(pipeline) && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(workerFailurePending(pipeline));
    }

    private static boolean workerFailurePending(DisruptorPipeline<?> pipeline) {
        Object lifecycleLock = lifecycleLock(pipeline);
        synchronized (lifecycleLock) {
            try {
                var field = pipeline.getClass().getDeclaredField("workerFailurePending");
                field.setAccessible(true);
                Object pending = field.get(pipeline);
                return pending instanceof Boolean value ? value : pending != null;
            } catch (ReflectiveOperationException failure) {
                throw new AssertionError("无法读取 worker failure pending 状态", failure);
            }
        }
    }

    private static void awaitThreadState(Thread thread, Thread.State state) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (thread.getState() != state && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(state, thread.getState());
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
