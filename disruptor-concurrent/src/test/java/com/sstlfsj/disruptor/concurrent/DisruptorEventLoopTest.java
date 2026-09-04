package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisruptorEventLoopTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    @Test
    void constructionRegistersButDoesNotStartWorkerAndStartSealsAfterThreadStart() throws Exception {
        AtomicReference<DisruptorEventLoop> loopRef = new AtomicReference<>();
        AtomicBoolean startCalled = new AtomicBoolean();
        AtomicBoolean sawStartingBeforeSeal = new AtomicBoolean();
        AtomicReference<Thread> workerRef = new AtomicReference<>();
        DisruptorEventLoop loop = EventLoopBuilder.bounded("orders", 8)
                .threadFactory(command -> {
                    Thread worker = new Thread(command, "orders-worker") {
                        @Override
                        public synchronized void start() {
                            EventLoopSnapshot snapshot = loopRef.get().snapshot();
                            sawStartingBeforeSeal.set(
                                    snapshot.lifecycle() == SupervisedLifecycle.STARTING
                                            && !snapshot.worker().registrationSealed());
                            startCalled.set(true);
                            super.start();
                        }
                    };
                    workerRef.set(worker);
                    return worker;
                })
                .shutdownTimeout(TEST_TIMEOUT)
                .build();
        loopRef.set(loop);

        assertEquals(SupervisedLifecycle.NEW, loop.snapshot().lifecycle());
        assertEquals(1, loop.snapshot().worker().registeredWorkers());
        assertFalse(loop.snapshot().worker().registrationSealed());
        assertFalse(workerRef.get().isAlive());
        assertFalse(startCalled.get());
        assertFalse(loop.tryExecute(() -> { }));
        assertThrows(RejectedExecutionException.class, () -> loop.execute(() -> { }));

        loop.start().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertTrue(startCalled.get());
        assertTrue(sawStartingBeforeSeal.get());
        assertTrue(loop.snapshot().worker().registrationSealed());
        assertEquals(SupervisedLifecycle.RUNNING, loop.snapshot().lifecycle());
        assertTrue(loop.snapshot().acceptingTasks());
        terminate(loop);
    }

    @Test
    void startCompletesOnlyAfterModuleStartsOnWorkerThread() throws Exception {
        CountDownLatch moduleEntered = new CountDownLatch(1);
        CountDownLatch releaseModule = new CountDownLatch(1);
        AtomicReference<Thread> moduleThread = new AtomicReference<>();
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        AtomicBoolean inEventLoop = new AtomicBoolean();
        EventLoopModule module = new EventLoopModule() {
            @Override
            public void onStart(EventLoop loop) throws Exception {
                moduleThread.set(Thread.currentThread());
                inEventLoop.set(loop.inEventLoop());
                moduleEntered.countDown();
                assertTrue(releaseModule.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            }
        };
        DisruptorEventLoop loop = EventLoopBuilder.bounded("modules", 8)
                .module(module)
                .threadFactory(command -> {
                    Thread worker = new Thread(command, "modules-worker");
                    workerThread.set(worker);
                    return worker;
                })
                .shutdownTimeout(TEST_TIMEOUT)
                .build();

        CompletableFuture<Void> startup = loop.start().toCompletableFuture();
        assertTrue(moduleEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertFalse(startup.isDone());
        assertEquals(SupervisedLifecycle.STARTING, loop.snapshot().lifecycle());
        assertFalse(loop.snapshot().acceptingTasks());
        releaseModule.countDown();
        startup.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertTrue(inEventLoop.get());
        assertSame(moduleThread.get(), workerThread.get());
        assertEquals(SupervisedLifecycle.RUNNING, loop.snapshot().lifecycle());
        terminate(loop);
    }

    @Test
    void threadStartFailureFailsStartupAndReallyTerminates() throws Exception {
        IllegalStateException original = new IllegalStateException("cannot start");
        DisruptorEventLoop loop = EventLoopBuilder.bounded("broken-start", 8)
                .threadFactory(command -> new Thread(command, "broken-start-worker") {
                    @Override
                    public synchronized void start() {
                        throw original;
                    }
                })
                .shutdownTimeout(TEST_TIMEOUT)
                .build();

        ExecutionException startupFailure = assertThrows(ExecutionException.class,
                () -> loop.start().toCompletableFuture()
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertSame(original, startupFailure.getCause());

        EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(SupervisedLifecycle.TERMINATED, terminated.lifecycle());
        assertSame(original, terminated.failure());
        assertEquals(0, terminated.worker().aliveWorkers());
    }

    @Test
    void partialModuleFailureRollsBackStartedModulesInReverseOrder() throws Exception {
        StringBuilder events = new StringBuilder();
        IllegalArgumentException original = new IllegalArgumentException("module failed");
        EventLoopModule first = new EventLoopModule() {
            @Override
            public void onStart(EventLoop loop) {
                events.append("start-first,");
            }

            @Override
            public void onStop(EventLoop loop) {
                events.append("stop-first");
            }
        };
        EventLoopModule second = new EventLoopModule() {
            @Override
            public void onStart(EventLoop loop) {
                events.append("start-second,");
                throw original;
            }
        };
        DisruptorEventLoop loop = EventLoopBuilder.bounded("failed-module", 8)
                .module(first)
                .module(second)
                .shutdownTimeout(TEST_TIMEOUT)
                .build();

        ExecutionException startupFailure = assertThrows(ExecutionException.class,
                () -> loop.start().toCompletableFuture()
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertSame(original, startupFailure.getCause());
        EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertSame(original, terminated.failure());
        assertEquals("start-first,start-second,stop-first", events.toString());
        assertEquals(0, terminated.worker().aliveWorkers());
    }

    @Test
    void shutdownDuringModuleStartupHasOneFailedStartupAndOneTerminationOutcome() throws Exception {
        CountDownLatch moduleEntered = new CountDownLatch(1);
        EventLoopModule blocking = new EventLoopModule() {
            @Override
            public void onStart(EventLoop loop) throws Exception {
                moduleEntered.countDown();
                new CountDownLatch(1).await();
            }
        };
        DisruptorEventLoop loop = EventLoopBuilder.bounded("startup-shutdown", 8)
                .module(blocking)
                .shutdownTimeout(TEST_TIMEOUT)
                .build();
        CompletableFuture<Void> startup = loop.start().toCompletableFuture();
        assertTrue(moduleEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        loop.shutdownNow();

        assertThrows(ExecutionException.class,
                () -> startup.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(SupervisedLifecycle.TERMINATED, terminated.lifecycle());
        assertFalse(terminated.acceptingTasks());
        assertEquals(0, terminated.worker().aliveWorkers());
    }

    private static void terminate(DisruptorEventLoop loop) throws Exception {
        loop.shutdownNow();
        loop.termination().toCompletableFuture()
                .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
