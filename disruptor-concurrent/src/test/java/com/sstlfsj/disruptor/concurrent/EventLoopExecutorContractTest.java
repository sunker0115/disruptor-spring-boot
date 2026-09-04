package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopExecutorContractTest {

    private static final long TIMEOUT_SECONDS = 2;

    @Test
    void executeAndAllSubmitOverloadsUseOneWorkerInAcceptedOrder() throws Exception {
        DisruptorEventLoop loop = runningLoop("executor", 16);
        List<Integer> order = new ArrayList<>();
        AtomicReference<Thread> worker = new AtomicReference<>();
        CountDownLatch executed = new CountDownLatch(1);

        loop.execute(() -> {
            worker.set(Thread.currentThread());
            order.add(1);
        });
        Future<?> runnable = loop.submit(() -> {
            order.add(2);
        });
        Future<String> runnableWithResult = loop.submit(() -> order.add(3), "result");
        Future<Integer> callable = loop.submit(() -> {
            order.add(4);
            assertSame(worker.get(), Thread.currentThread());
            executed.countDown();
            return 4;
        });

        assertEquals(null, runnable.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("result", runnableWithResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(4, callable.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertTrue(executed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(List.of(1, 2, 3, 4), order);
        awaitCondition(() -> loop.snapshot().completedTasks() == 4);
        assertEquals(4, loop.snapshot().completedTasks());
        terminate(loop);
    }

    @Test
    void boundedCapacityCoversTheRunningTaskAndRecoversOnlyAfterPhysicalCompletion()
            throws Exception {
        DisruptorEventLoop loop = runningLoop("capacity", 1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> running = loop.submit(() -> {
            entered.countDown();
            await(release);
        });
        assertTrue(entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        assertEquals(1, loop.snapshot().outstandingTasks());
        assertFalse(loop.tryExecute(() -> { }));
        RejectedExecutionException rejected = assertThrows(
                RejectedExecutionException.class, () -> loop.execute(() -> { }));
        assertTrue(rejected.getMessage().contains("outstanding=1"));

        release.countDown();
        running.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        awaitCondition(() -> loop.snapshot().outstandingTasks() == 0);
        CountDownLatch accepted = new CountDownLatch(1);
        assertTrue(loop.tryExecute(accepted::countDown));
        assertTrue(accepted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        terminate(loop);
    }

    @Test
    void nakedFailureUsesHandlerButSubmittedFailureStaysOnlyInFuture() throws Exception {
        AtomicInteger handled = new AtomicInteger();
        AtomicReference<Runnable> handledCommand = new AtomicReference<>();
        AtomicReference<Throwable> handledFailure = new AtomicReference<>();
        DisruptorEventLoop loop = EventLoopBuilder.bounded("failures", 8)
                .taskExceptionHandler((eventLoop, command, failure) -> {
                    handled.incrementAndGet();
                    handledCommand.set(command);
                    handledFailure.set(failure);
                })
                .build();
        start(loop);
        IllegalStateException nakedFailure = new IllegalStateException("naked");
        Runnable naked = () -> {
            throw nakedFailure;
        };
        CountDownLatch afterFailure = new CountDownLatch(1);

        loop.execute(naked);
        Future<?> submitted = loop.submit(() -> {
            throw new IllegalArgumentException("submitted");
        });
        loop.execute(afterFailure::countDown);

        assertTrue(afterFailure.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        ExecutionException submittedFailure = assertThrows(ExecutionException.class,
                () -> submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("submitted", submittedFailure.getCause().getMessage());
        assertEquals(1, handled.get());
        assertSame(naked, handledCommand.get());
        assertSame(nakedFailure, handledFailure.get());
        assertEquals(2, loop.snapshot().failedTasks());
        terminate(loop);
    }

    @Test
    void taskSubmissionIsRejectedBeforeStartAndAfterShutdown() throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder.bounded("rejection", 8).build();

        assertFalse(loop.tryExecute(() -> { }));
        RejectedExecutionException before = assertThrows(
                RejectedExecutionException.class, () -> loop.submit(() -> { }));
        assertTrue(before.getMessage().contains("lifecycle=NEW"));

        start(loop);
        loop.shutdown();
        assertTrue(loop.isShutdown());
        assertFalse(loop.tryExecute(() -> { }));
        assertThrows(RejectedExecutionException.class, () -> loop.submit(() -> { }));
        assertTrue(loop.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertTrue(loop.isTerminated());
    }

    private static DisruptorEventLoop runningLoop(String name, int capacity) throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder.bounded(name, capacity)
                .shutdownTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .build();
        start(loop);
        return loop;
    }

    private static void start(DisruptorEventLoop loop) throws Exception {
        loop.start().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void terminate(DisruptorEventLoop loop) throws Exception {
        loop.shutdownNow();
        loop.termination().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
        }
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("等待条件超时");
            }
            Thread.sleep(1);
        }
    }
}
