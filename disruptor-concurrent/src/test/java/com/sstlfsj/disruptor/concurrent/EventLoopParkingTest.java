package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopParkingTest {

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"bounded", "unbounded"})
    void spuriousUnparkRechecksWorkAndDoesNotLoseTheNextPublication(String backend)
            throws Exception {
        AtomicReference<Thread> worker = new AtomicReference<>();
        EventLoop loop = (backend.equals("bounded")
                ? EventLoopBuilder.bounded("spurious-parking", 8)
                : EventLoopBuilder.unbounded("spurious-parking", 8))
                .threadFactory(command -> {
                    Thread thread = Thread.ofPlatform().name("spurious-parking-worker")
                            .unstarted(command);
                    worker.set(thread);
                    return thread;
                }).build();
        try {
            loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
            var threads = ManagementFactory.getThreadMXBean();
            for (int round = 0; round < 16; round++) {
                await(() -> worker.get().getState() == Thread.State.WAITING);
                long previousWaits = threads.getThreadInfo(worker.get().threadId()).getWaitedCount();
                LockSupport.unpark(worker.get());
                // 等待次数增长证明发生了新一轮 park，不能误把旧 WAITING 当作已复查。
                await(() -> worker.get().getState() == Thread.State.WAITING
                        && threads.getThreadInfo(worker.get().threadId()).getWaitedCount() > previousWaits);
                assertEquals(round, loop.snapshot().completedTasks());
                assertTrue(loop.submit(loop::inEventLoop).get(2, TimeUnit.SECONDS));
            }
            await(() -> worker.get().getState() == Thread.State.WAITING);
            assertEquals(16, loop.snapshot().completedTasks());
            assertEquals(0, loop.snapshot().outstandingTasks());
        } finally {
            loop.shutdownNow();
            loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "{0}, future timer = {1}")
    @CsvSource({"bounded, false", "unbounded, false", "bounded, true", "unbounded, true"})
    void idleWorkerWakesForIngressEarlierTimerCancellationAndShutdown(
            String backend, boolean withFutureTimer) throws Exception {
        AtomicReference<Thread> worker = new AtomicReference<>();
        EventLoop loop = (backend.equals("bounded")
                ? EventLoopBuilder.bounded("parking", 8)
                : EventLoopBuilder.unbounded("parking", 8))
                .threadFactory(runnable -> {
                    Thread thread = Thread.ofPlatform().name("parking-worker").unstarted(runnable);
                    worker.set(thread);
                    return thread;
                }).build();
        try {
            loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
            ScheduledFuture<?> futureTimer = withFutureTimer
                    ? loop.schedule(() -> { }, 1, TimeUnit.DAYS) : null;
            Thread.State expectedPark = withFutureTimer
                    ? Thread.State.TIMED_WAITING : Thread.State.WAITING;
            await(() -> worker.get().getState() == expectedPark
                    && loop.snapshot().scheduledPendingTasks() == (withFutureTimer ? 1 : 0));

            assertEquals("ingress", loop.submit(() -> "ingress").get(2, TimeUnit.SECONDS));
            await(() -> worker.get().getState() == expectedPark);
            assertEquals("earlier", loop.schedule(() -> "earlier", 1, TimeUnit.MILLISECONDS)
                    .get(2, TimeUnit.SECONDS));

            if (futureTimer != null) {
                await(() -> worker.get().getState() == Thread.State.TIMED_WAITING);
                assertTrue(futureTimer.cancel(false));
            }
            await(() -> loop.snapshot().outstandingTasks() == 0
                    && worker.get().getState() == Thread.State.WAITING);
            loop.shutdown();
            EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                    .get(2, TimeUnit.SECONDS);
            assertTrue(terminated.worker().gracefulTermination());
            assertEquals(0, terminated.outstandingTasks());
        } finally {
            if (!loop.isTerminated()) {
                loop.shutdownNow();
                loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() - deadline < 0, "等待 worker park 超时");
            Thread.sleep(1);
        }
    }
}
