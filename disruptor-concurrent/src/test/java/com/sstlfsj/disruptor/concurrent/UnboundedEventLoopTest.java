package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnboundedEventLoopTest {

    @ParameterizedTest
    @ValueSource(strings = {"poll", "advanceConsumer"})
    void shutdownScannerUsesSlotOwnershipWhileWorkerIsPausedInsideQueueAccess(String pausedMethod)
            throws Exception {
        UnboundedEventLoop loop = EventLoopBuilder.unbounded("scanner-quiescence", 1)
                .maxPooledSegments(0)
                .build();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch finishRunning = new CountDownLatch(1);
        CountDownLatch pollEntered = new CountDownLatch(1);
        CountDownLatch allowPoll = new CountDownLatch(1);
        CountDownLatch scannerEntered = new CountDownLatch(1);
        CountDownLatch scannerFinished = new CountDownLatch(1);
        AtomicBoolean interceptPoll = new AtomicBoolean();

        Field kernelField = AbstractEventLoop.class.getDeclaredField("kernel");
        kernelField.setAccessible(true);
        Object kernel = kernelField.get(loop);
        Field queueField = kernel.getClass().getDeclaredField("queue");
        queueField.setAccessible(true);
        Object queue = queueField.get(kernel);
        Class<?> queueType = queueField.getType();
        // 只插入调度屏障；claim、publication、poll、回收和扫描均委托真实队列。
        queueField.set(kernel, Proxy.newProxyInstance(queueType.getClassLoader(),
                new Class<?>[]{queueType}, (proxy, method, arguments) -> {
                    method.setAccessible(true);
                    boolean delayedPoll = method.getName().equals(pausedMethod)
                            && interceptPoll.compareAndSet(true, false);
                    if (delayedPoll) {
                        pollEntered.countDown();
                        awaitIgnoringInterrupt(allowPoll);
                    }
                    boolean scanning = method.getName().equals("scanOrdinaryUnstarted");
                    if (scanning) {
                        scannerEntered.countDown();
                    }
                    try {
                        return method.invoke(queue, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    } finally {
                        if (scanning) {
                            scannerFinished.countDown();
                        }
                    }
                }));

        var shutdownCaller = Executors.newSingleThreadExecutor();
        try {
            loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
            loop.execute(() -> {
                running.countDown();
                awaitIgnoringInterrupt(finishRunning);
            });
            assertTrue(running.await(2, TimeUnit.SECONDS));
            loop.execute(() -> { });
            loop.execute(() -> { });
            interceptPoll.set(true);
            finishRunning.countDown();
            assertTrue(pollEntered.await(2, TimeUnit.SECONDS));
            // worker 此时已通过 canProcessTasks，尚未执行真实 poll。
            var shutdown = shutdownCaller.submit(loop::shutdownNow);
            awaitCondition(loop::isShutdown);
            try {
                assertTrue(scannerEntered.await(2, TimeUnit.SECONDS),
                        "publisher 排空后 scanner 不应等待 worker 的逐任务静默票据");
                assertTrue(scannerFinished.await(2, TimeUnit.SECONDS),
                        "不可变段链上的 scanner 应能在 worker 暂停时独立完成");
            } finally {
                allowPoll.countDown();
            }
            int expectedReturned = pausedMethod.equals("poll") ? 2 : 1;
            assertEquals(expectedReturned, shutdown.get(2, TimeUnit.SECONDS).size(),
                    "槽位 WAITING/RUNNING CAS 必须精确决定 shutdownNow 所有权");
        } finally {
            finishRunning.countDown();
            allowPoll.countDown();
            loop.shutdownNow();
            assertTrue(loop.awaitTermination(2, TimeUnit.SECONDS));
            shutdownCaller.shutdownNow();
            assertTrue(shutdownCaller.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void builderRequiresPowerOfTwoSegmentSize() {
        assertThrows(IllegalArgumentException.class,
                () -> EventLoopBuilder.unbounded("invalid", 0));
        assertThrows(IllegalArgumentException.class,
                () -> EventLoopBuilder.unbounded("invalid", 3));
        assertThrows(IllegalArgumentException.class,
                () -> EventLoopBuilder.unbounded("invalid-pool", 4)
                        .maxPooledSegments(-1));
        assertThrows(IllegalStateException.class,
                () -> EventLoopBuilder.bounded("bounded-pool", 4)
                        .maxPooledSegments(0));
    }

    @Test
    void maxPooledSegmentsZeroDisablesPooling() throws Exception {
        UnboundedEventLoop loop = EventLoopBuilder.unbounded("no-segment-pool", 1)
                .maxPooledSegments(0)
                .build();
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        CountDownLatch blocker = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        try {
            loop.execute(() -> {
                entered.countDown();
                await(blocker);
            });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            for (int index = 0; index < 4; index++) {
                loop.execute(() -> { });
            }
            EventLoopSnapshot expanded = loop.snapshot();
            assertEquals(5, expanded.activeQueueSegments());
            assertEquals(5, expanded.allocatedQueueSegments());

            blocker.countDown();
            awaitCondition(() -> loop.snapshot().outstandingTasks() == 0);
            EventLoopSnapshot drained = loop.snapshot();
            assertEquals(1, drained.activeQueueSegments());
            assertEquals(1, drained.allocatedQueueSegments());
            loop.shutdown();
            loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        } finally {
            blocker.countDown();
            if (!loop.isTerminated()) {
                loop.shutdownNow();
                loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void builderDefaultsToEightPooledSegments() throws Exception {
        UnboundedEventLoop loop = EventLoopBuilder.unbounded("default-segment-pool", 1)
                .build();
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        CountDownLatch blocker = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        try {
            loop.execute(() -> {
                entered.countDown();
                await(blocker);
            });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            for (int index = 0; index < 12; index++) {
                loop.execute(() -> { });
            }

            blocker.countDown();
            awaitCondition(() -> loop.snapshot().outstandingTasks() == 0);
            EventLoopSnapshot drained = loop.snapshot();
            assertEquals(1, drained.activeQueueSegments());
            assertEquals(9, drained.allocatedQueueSegments());
            loop.shutdown();
            loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        } finally {
            blocker.countDown();
            if (!loop.isTerminated()) {
                loop.shutdownNow();
                loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void snapshotExpressesUnboundedCapacityAndRealSegmentAllocation() throws Exception {
        UnboundedEventLoop loop = EventLoopBuilder.unbounded("unbounded", 4).build();
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        CountDownLatch blocker = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        loop.execute(() -> {
            entered.countDown();
            await(blocker);
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        for (int index = 0; index < 12; index++) {
            loop.execute(() -> { });
        }

        EventLoopSnapshot snapshot = loop.snapshot();
        assertEquals(CapacityMode.UNBOUNDED, snapshot.capacityMode());
        assertEquals(OptionalLong.empty(), snapshot.capacityLimit());
        assertEquals(OptionalLong.empty(), snapshot.remainingCapacity());
        assertTrue(snapshot.allocatedQueueSegments() >= 3);
        assertTrue(snapshot.activeQueueSegments() >= 3);
        assertTrue(snapshot.activeQueueSegments() <= snapshot.allocatedQueueSegments());
        blocker.countDown();
        loop.shutdown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
        }
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
}
