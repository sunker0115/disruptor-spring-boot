package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopBlockingGuardTest {

    @Test
    void rejectsEveryBlockingPathFromOwnerLoopButAllowsCompletedFutureRead() throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder.bounded("blocking-guard", 16).build();
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        Future<String> completed = loop.submit(() -> "ready");
        assertEquals("ready", completed.get(2, TimeUnit.SECONDS));
        CountDownLatch checked = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        loop.execute(() -> {
            try {
                assertEquals("ready", completed.get());
                assertBlocking(() -> loop.submit(() -> "later").get());
                assertBlocking(() -> loop.submit(() -> "later").get(1, TimeUnit.SECONDS));
                assertBlocking(() -> loop.awaitTermination(1, TimeUnit.MILLISECONDS));
                assertBlocking(() -> loop.invokeAll(List.of(() -> 1)));
                assertBlocking(() -> loop.invokeAny(List.of(() -> 1)));
                assertBlocking(loop::close);
            } catch (Throwable thrown) {
                failure.set(thrown);
            } finally {
                checked.countDown();
            }
        });

        assertTrue(checked.await(2, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        loop.shutdownNow();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static void assertBlocking(ThrowingAction action) {
        Throwable thrown;
        try {
            action.run();
            thrown = null;
        } catch (Throwable failure) {
            thrown = failure;
        }
        assertInstanceOf(BlockingOperationException.class, thrown);
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }
}
