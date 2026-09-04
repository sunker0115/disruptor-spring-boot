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
}
