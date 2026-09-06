package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnboundedEventLoopTest {

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
