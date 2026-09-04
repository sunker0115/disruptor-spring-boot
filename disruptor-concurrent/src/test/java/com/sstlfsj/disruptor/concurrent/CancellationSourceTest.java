package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CancellationSourceTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    @Test
    void firstCancellationWinsAndFreezesReason() {
        CancellationSource source = new CancellationSource();
        CancellationReason first = CancellationReason.of("first");

        assertTrue(source.cancel(first));
        assertFalse(source.cancel(CancellationReason.of("second")));
        assertTrue(source.isCancellationRequested());
        assertSame(first, source.cancellationReason().orElseThrow());
    }

    @Test
    void lateSynchronousListenerRunsOnRegisteringThread() {
        CancellationSource source = new CancellationSource();
        source.cancel(CancellationReason.of("done"));
        Thread registeringThread = Thread.currentThread();
        AtomicReference<Thread> invokedThread = new AtomicReference<>();

        CancellationRegistration registration = source.onCancellation(
                reason -> invokedThread.set(Thread.currentThread()));

        assertSame(registeringThread, invokedThread.get());
        assertFalse(registration.unregister());
    }

    @Test
    void asynchronousListenerRunsOnProvidedExecutor() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "cancel-listener"));
        try {
            CancellationSource source = new CancellationSource();
            CountDownLatch invoked = new CountDownLatch(1);
            AtomicReference<String> threadName = new AtomicReference<>();
            source.onCancellation(executor, reason -> {
                threadName.set(Thread.currentThread().getName());
                invoked.countDown();
            });

            source.cancel(CancellationReason.of("async"));

            assertTrue(invoked.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals("cancel-listener", threadName.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void successfulUnregisterPreventsInvocationAndUnlinksNode() {
        CancellationSource source = new CancellationSource();
        AtomicInteger invocations = new AtomicInteger();
        CancellationRegistration registration = source.onCancellation(
                reason -> invocations.incrementAndGet());

        assertTrue(registration.unregister());
        assertFalse(registration.unregister());
        source.cancel(CancellationReason.of("after-unregister"));

        assertEquals(0, invocations.get());
        assertEquals(0, source.listenerCount());
    }

    @Test
    void cancelAndUnregisterLinearizeWithoutDoubleInvocation() throws Exception {
        ExecutorService racers = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < 200; iteration++) {
                CancellationSource source = new CancellationSource();
                AtomicInteger invocations = new AtomicInteger();
                CancellationRegistration registration = source.onCancellation(
                        reason -> invocations.incrementAndGet());
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> cancelled = racers.submit(() -> {
                    start.await();
                    return source.cancel(CancellationReason.of("race"));
                });
                Future<Boolean> unregistered = racers.submit(() -> {
                    start.await();
                    return registration.unregister();
                });

                start.countDown();
                assertTrue(cancelled.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                boolean removed = unregistered.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertEquals(removed ? 0 : 1, invocations.get());
                assertEquals(0, source.listenerCount());
            }
        } finally {
            racers.shutdownNow();
        }
    }

    @Test
    void listenerFailureDoesNotSkipRemainingListeners() {
        List<Throwable> failures = new ArrayList<>();
        CancellationSource source = new CancellationSource((reason, failure) -> failures.add(failure));
        AtomicInteger later = new AtomicInteger();
        IllegalStateException original = new IllegalStateException("boom");
        source.onCancellation(reason -> {
            throw original;
        });
        source.onCancellation(reason -> later.incrementAndGet());

        source.cancel(CancellationReason.of("notify"));

        assertEquals(List.of(original), failures);
        assertEquals(1, later.get());
    }

    @Test
    void rejectedListenerExecutorIsReportedWithoutChangingReason() {
        List<Throwable> failures = new ArrayList<>();
        CancellationSource source = new CancellationSource((reason, failure) -> failures.add(failure));
        CancellationReason reason = CancellationReason.of("frozen");
        source.cancel(reason);
        RejectedExecutionException rejected = new RejectedExecutionException("closed");

        source.onCancellation(command -> {
            throw rejected;
        }, ignored -> {
        });

        assertEquals(List.of(rejected), failures);
        assertSame(reason, source.cancellationReason().orElseThrow());
    }

    @Test
    void cancelAfterUsesExplicitSchedulerAndCancelsTimerWhenSourceWinsFirst() throws Exception {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setRemoveOnCancelPolicy(true);
        try {
            CancellationSource source = new CancellationSource();
            CancellationReason first = CancellationReason.of("manual");
            source.cancelAfter(CancellationReason.of("timer"), Duration.ofMinutes(1), scheduler);

            assertTrue(source.cancel(first));
            awaitCondition(() -> scheduler.getQueue().isEmpty());

            assertSame(first, source.cancellationReason().orElseThrow());
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static void awaitCondition(Check check) throws Exception {
        long deadline = System.nanoTime() + TEST_TIMEOUT.toNanos();
        while (!check.evaluate()) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("等待条件超时");
            }
            Thread.onSpinWait();
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean evaluate() throws Exception;
    }
}
