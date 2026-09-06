package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import com.sstlfsj.disruptor.core.WorkerSupervisor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopShutdownTest {

    @Test
    void concurrentShutdownNowDoesNotSplitReturnedSet() throws Exception {
        var callers = Executors.newFixedThreadPool(2);
        try {
            for (int attempt = 0; attempt < 40; attempt++) {
                DisruptorEventLoop loop = runningLoop("concurrent-now-" + attempt, 64);
                CountDownLatch entered = new CountDownLatch(1);
                CountDownLatch release = new CountDownLatch(1);
                CountDownLatch start = new CountDownLatch(1);
                try {
                    loop.execute(() -> {
                        entered.countDown();
                        awaitIgnoringInterrupt(release);
                    });
                    assertTrue(entered.await(2, TimeUnit.SECONDS));
                    List<Runnable> queued = new ArrayList<>();
                    for (int i = 0; i < 20; i++) {
                        Runnable task = new NamedRunnable("queued-" + i);
                        queued.add(task);
                        loop.execute(task);
                    }
                    Future<List<Runnable>> first = callers.submit(() -> {
                        start.await();
                        return loop.shutdownNow();
                    });
                    Future<List<Runnable>> second = callers.submit(() -> {
                        start.await();
                        return loop.shutdownNow();
                    });
                    start.countDown();
                    List<Runnable> a = first.get(2, TimeUnit.SECONDS);
                    List<Runnable> b = second.get(2, TimeUnit.SECONDS);
                    List<Runnable> union = new ArrayList<>(a);
                    union.addAll(b);

                    assertEquals(20, union.size());
                    assertTrue(a.isEmpty() || b.isEmpty(), "返回集合必须由一个调用方完整取得");
                    assertEquals(queued, union);
                } finally {
                    start.countDown();
                    release.countDown();
                    loop.shutdownNow();
                    assertTrue(loop.awaitTermination(2, TimeUnit.SECONDS));
                }
            }
        } finally {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void stuckWorkerStillCancelsQueuedFutureOnImmediateDeadline() throws Exception {
        DisruptorEventLoop loop = runningLoop("stuck", 16);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            loop.execute(() -> {
                entered.countDown();
                awaitIgnoringInterrupt(release);
            });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<?> queued = loop.submit(() -> { });

            loop.requestShutdown(ShutdownMode.IMMEDIATE,
                    ShutdownDeadline.after(Duration.ofMillis(100)));

            awaitCondition(queued::isCancelled);
            assertFalse(loop.isTerminated());
        } finally {
            release.countDown();
            loop.shutdownNow();
            assertTrue(loop.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void shutdownUsesUnboundedDeadlineAndRunsAcceptedFutureOneShot() throws Exception {
        ManualNanoClock clock = new ManualNanoClock();
        AtomicReference<Thread> worker = new AtomicReference<>();
        DisruptorEventLoop loop = EventLoopBuilder.bounded("orderly", 8)
                .clock(clock)
                .threadFactory(command -> {
                    Thread thread = new Thread(command, "orderly-worker");
                    worker.set(thread);
                    return thread;
                })
                .build();
        start(loop);
        ScheduledFuture<String> delayed = loop.schedule(() -> "kept", 1, TimeUnit.DAYS);
        awaitCondition(() -> loop.snapshot().scheduledPendingTasks() == 1);

        loop.shutdown();

        assertTrue(loop.isShutdown());
        assertFalse(loop.awaitTermination(50, TimeUnit.MILLISECONDS));
        assertFalse(delayed.isDone());
        clock.set(Duration.ofDays(1).toNanos());
        java.util.concurrent.locks.LockSupport.unpark(worker.get());

        assertEquals("kept", delayed.get(2, TimeUnit.SECONDS));
        assertTrue(loop.awaitTermination(2, TimeUnit.SECONDS));
        assertEquals(ShutdownMode.GRACEFUL, loop.snapshot().shutdownMode());
        assertTrue(loop.snapshot().worker().gracefulTermination());
    }

    @Test
    void shutdownCancelsWaitingPeriodicButDoesNotInterruptCurrentInvocation() throws Exception {
        DisruptorEventLoop loop = runningLoop("periodic-shutdown", 8);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        ScheduledFuture<?> periodic = loop.scheduleAtFixedRate(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException failure) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        }, 0, 1, TimeUnit.DAYS);
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        loop.shutdown();
        Thread.sleep(50);

        assertFalse(interrupted.get());
        assertFalse(loop.isTerminated());
        release.countDown();
        assertTrue(loop.awaitTermination(2, TimeUnit.SECONDS));
        assertTrue(periodic.isCancelled());
        assertEquals(CancellationReason.SHUTDOWN,
                ((EventLoopScheduledFuture<?>) periodic).snapshot().cancellationReason());
    }

    @Test
    void shutdownNowReturnsWaitingOriginalRunnablesInAcceptedOrder() throws Exception {
        DisruptorEventLoop loop = runningLoop("shutdown-now-order", 16);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> running = loop.submit(() -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        NamedRunnable first = new NamedRunnable("first");
        NamedRunnable third = new NamedRunnable("third");
        loop.execute(first);
        Future<String> callable = loop.submit(() -> "second");
        loop.schedule(third, 1, TimeUnit.DAYS);

        List<Runnable> returned = loop.shutdownNow();

        assertEquals(3, returned.size());
        assertSame(first, returned.get(0));
        assertSame(callable, returned.get(1));
        assertSame(third, returned.get(2));
        assertTrue(callable.isCancelled());
        assertFalse(returned.contains(running));
        release.countDown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void shutdownNowDoesNotReturnRunningTaskAndInterruptsWorker() throws Exception {
        DisruptorEventLoop loop = runningLoop("shutdown-now-running", 8);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> running = loop.submit(() -> {
            entered.countDown();
            while (release.getCount() != 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    interrupted.countDown();
                }
            }
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        List<Runnable> returned = loop.shutdownNow();

        assertEquals(List.of(), returned);
        assertTrue(interrupted.await(2, TimeUnit.SECONDS));
        assertTrue(running.isCancelled());
        assertFalse(loop.isTerminated());
        release.countDown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void boundedGracefulDeadlineEscalatesWithoutFakingTermination() throws Exception {
        DisruptorEventLoop loop = runningLoop("deadline", 8);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        loop.execute(() -> {
            entered.countDown();
            while (release.getCount() != 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    interrupted.countDown();
                }
            }
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        loop.requestShutdown(
                ShutdownMode.GRACEFUL,
                ShutdownDeadline.after(Duration.ofMillis(50)));
        assertTrue(interrupted.await(2, TimeUnit.SECONDS));
        awaitCondition(() -> loop.snapshot().shutdownMode() == ShutdownMode.IMMEDIATE);

        EventLoopSnapshot stopping = loop.snapshot();
        assertEquals(SupervisedLifecycle.STOPPING, stopping.lifecycle());
        assertInstanceOf(WorkerSupervisor.WorkerTerminationTimeoutException.class,
                stopping.failure());
        assertFalse(loop.termination().toCompletableFuture().isDone());
        release.countDown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void terminationWaitsForInterruptIgnoringTaskToReallyExit() throws Exception {
        DisruptorEventLoop loop = runningLoop("real-termination", 8);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        loop.execute(() -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        loop.shutdownNow();
        Thread.sleep(50);

        assertFalse(loop.isTerminated());
        assertFalse(loop.termination().toCompletableFuture().isDone());
        assertEquals(1, loop.snapshot().worker().aliveWorkers());
        release.countDown();
        EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertEquals(SupervisedLifecycle.TERMINATED, terminated.lifecycle());
        assertEquals(0, terminated.worker().aliveWorkers());
    }

    @Test
    void everyAcceptedFutureIsTerminalAndEveryReferenceIsCleared() throws Exception {
        DisruptorEventLoop loop = runningLoop("cleanup", 16);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> running = loop.submit(() -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        Future<?> queued = loop.submit(() -> { });
        ScheduledFuture<?> delayed = loop.schedule(() -> { }, 1, TimeUnit.DAYS);
        ScheduledFuture<?> periodic = loop.scheduleAtFixedRate(
                () -> { }, 1, 1, TimeUnit.DAYS);

        loop.shutdownNow();
        release.countDown();
        EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertTrue(running.isDone());
        assertTrue(queued.isDone());
        assertTrue(delayed.isDone());
        assertTrue(periodic.isDone());
        assertEquals(0, terminated.outstandingTasks());
        assertEquals(0, terminated.ingressPendingTasks());
        assertEquals(0, terminated.scheduledPendingTasks());
        assertEquals(0, terminated.executingTasks());
    }

    @Test
    void shutdownNowReturnsQueuedOrdinaryButNeverTheRunningOrdinary() throws Exception {
        DisruptorEventLoop loop = runningLoop("ordinary-shutdown-race", 8);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Runnable running = () -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        };
        NamedRunnable queued = new NamedRunnable("queued");
        loop.execute(running);
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        loop.execute(queued);

        List<Runnable> returned = loop.shutdownNow();

        assertEquals(List.of(queued), returned);
        assertFalse(returned.contains(running));
        release.countDown();
        EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertEquals(1, terminated.completedTasks());
        assertEquals(1, terminated.cancelledTasks());
        assertEquals(1, terminated.shutdownNowReturnedTasks());
    }

    private static DisruptorEventLoop runningLoop(String name, int capacity) throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder.bounded(name, capacity)
                .shutdownTimeout(Duration.ofSeconds(2))
                .build();
        start(loop);
        return loop;
    }

    private static void start(DisruptorEventLoop loop) throws Exception {
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        while (latch.getCount() != 0) {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("等待条件超时");
            }
            Thread.sleep(1);
        }
    }

    private record NamedRunnable(String name) implements Runnable {
        @Override
        public void run() {
        }
    }

    private static final class ManualNanoClock implements NanoClock {
        private final AtomicLong now = new AtomicLong();

        @Override
        public long nanoTime() {
            return now.get();
        }

        private void set(long value) {
            now.set(value);
        }
    }
}
