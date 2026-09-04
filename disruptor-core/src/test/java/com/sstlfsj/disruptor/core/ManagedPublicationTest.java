package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedPublicationTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final EventTranslatorOneArg<TestEvent, Integer> SET_VALUE =
            (event, sequence, value) -> event.value = value;

    @Test
    void supportsBoundedAndNonBlockingZeroToThreeArgumentTranslators() throws Exception {
        CountDownLatch consumed = new CountDownLatch(8);
        List<Integer> values = new CopyOnWriteArrayList<>();
        DisruptorPipeline<TestEvent> pipeline = pipeline("overloads", 16,
                (event, sequence, endOfBatch) -> {
                    values.add(event.value);
                    consumed.countDown();
                });
        PipelineHandle<TestEvent> handle = pipeline.handle();
        EventTranslator<TestEvent> zero = (event, sequence) -> event.value = 0;
        EventTranslatorOneArg<TestEvent, Integer> one =
                (event, sequence, value) -> event.value = value;
        EventTranslatorTwoArg<TestEvent, Integer, Integer> two =
                (event, sequence, left, right) -> event.value = left + right;
        EventTranslatorThreeArg<TestEvent, Integer, Integer, Integer> three =
                (event, sequence, first, second, third) -> event.value = first + second + third;
        try {
            start(pipeline);

            assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(zero));
            assertEquals(PublicationResult.PUBLISHED,
                    handle.publishEvent(zero, Duration.ZERO),
                    "零超时仍应完成一次立即发布尝试");
            assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(one, 1));
            assertEquals(PublicationResult.PUBLISHED,
                    handle.publishEvent(one, 2, Duration.ofSeconds(Long.MAX_VALUE)),
                    "超大 Duration 的纳秒换算和 deadline 加法必须饱和");
            assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(two, 1, 2));
            assertEquals(PublicationResult.PUBLISHED,
                    handle.publishEvent(two, 2, 2, TEST_TIMEOUT));
            assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(three, 1, 2, 3));
            assertEquals(PublicationResult.PUBLISHED,
                    handle.publishEvent(three, 2, 3, 4, TEST_TIMEOUT));

            assertTrue(consumed.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(0, 0, 1, 2, 3, 4, 6, 9), values);
        } finally {
            terminate(pipeline);
        }
    }

    @Test
    void conveniencePublishersReturnExplicitResults() throws Exception {
        CountDownLatch consumed = new CountDownLatch(2);
        DisruptorPipeline<TestEvent> pipeline = pipeline("convenience", 4,
                (event, sequence, endOfBatch) -> consumed.countDown());
        try {
            start(pipeline);

            assertEquals(PublicationResult.PUBLISHED,
                    pipeline.handle().tryPublish(event -> event.value = 1));
            assertEquals(PublicationResult.PUBLISHED,
                    pipeline.handle().publish(event -> event.value = 2, TEST_TIMEOUT));
            assertTrue(consumed.await(2, TimeUnit.SECONDS));
        } finally {
            terminate(pipeline);
        }
    }

    @Test
    void nonBlockingPublicationReportsCapacityWithoutInvokingTranslator() throws Exception {
        BlockedConsumer blocked = new BlockedConsumer();
        DisruptorPipeline<TestEvent> pipeline = pipeline("capacity", 4, blocked::onEvent);
        try {
            start(pipeline);
            fillRing(pipeline.handle(), blocked);
            AtomicBoolean translated = new AtomicBoolean();

            PublicationResult result = pipeline.handle().tryPublishEvent(
                    (event, sequence) -> translated.set(true));

            assertEquals(PublicationResult.CAPACITY_EXHAUSTED, result);
            assertFalse(translated.get());
        } finally {
            blocked.release();
            terminate(pipeline);
        }
    }

    @Test
    void boundedPublicationTimesOutAndValidatesDuration() throws Exception {
        BlockedConsumer blocked = new BlockedConsumer();
        DisruptorPipeline<TestEvent> pipeline = pipeline("timeout", 4, blocked::onEvent);
        try {
            start(pipeline);
            fillRing(pipeline.handle(), blocked);
            AtomicInteger translations = new AtomicInteger();
            EventTranslator<TestEvent> translator =
                    (event, sequence) -> translations.incrementAndGet();

            assertEquals(PublicationResult.TIMED_OUT,
                    pipeline.handle().publishEvent(translator, Duration.ZERO));
            long startedAt = System.nanoTime();
            assertEquals(PublicationResult.TIMED_OUT,
                    pipeline.handle().publishEvent(translator, Duration.ofMillis(20)));
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
            assertTrue(elapsed.compareTo(Duration.ofMillis(20)) >= 0,
                    "满环等待不能提前越过 deadline，实际=" + elapsed);
            assertTrue(elapsed.compareTo(Duration.ofSeconds(1)) < 0,
                    "满环等待必须在有界 deadline 后返回，实际=" + elapsed);
            assertEquals(0, translations.get());
            assertThrows(IllegalArgumentException.class,
                    () -> pipeline.handle().publishEvent(translator, Duration.ofNanos(-1)));
            assertThrows(NullPointerException.class,
                    () -> pipeline.handle().publishEvent(translator, null));
        } finally {
            blocked.release();
            terminate(pipeline);
        }
    }

    @RepeatedTest(5)
    void boundedPublicationWaitsForCapacityThenSucceeds() throws Exception {
        BlockedConsumer blocked = new BlockedConsumer();
        DisruptorPipeline<TestEvent> pipeline = pipeline("wait-capacity", 4, blocked::onEvent);
        AtomicReference<PublicationResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread publisher = null;
        try {
            start(pipeline);
            fillRing(pipeline.handle(), blocked);
            publisher = Thread.ofPlatform().name("bounded-capacity-publisher").start(() -> {
                try {
                    result.set(pipeline.handle().publishEvent(SET_VALUE, 99, TEST_TIMEOUT));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            });

            awaitThreadWaiting(publisher);
            blocked.release();
            publisher.join(2_000);

            assertFalse(publisher.isAlive());
            assertNull(failure.get());
            assertEquals(PublicationResult.PUBLISHED, result.get());
        } finally {
            blocked.release();
            if (publisher != null) {
                publisher.interrupt();
                publisher.join(2_000);
            }
            terminate(pipeline);
        }
    }

    @Test
    void boundedPublicationIsInterruptible() throws Exception {
        BlockedConsumer blocked = new BlockedConsumer();
        DisruptorPipeline<TestEvent> pipeline = pipeline("interrupt", 4, blocked::onEvent);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptedStatusInCatch = new AtomicBoolean(true);
        Thread publisher = null;
        try {
            start(pipeline);
            fillRing(pipeline.handle(), blocked);
            publisher = Thread.ofPlatform().name("interruptible-publisher").start(() -> {
                try {
                    pipeline.handle().publishEvent(SET_VALUE, 99, Duration.ofSeconds(10));
                } catch (Throwable thrown) {
                    interruptedStatusInCatch.set(Thread.currentThread().isInterrupted());
                    failure.set(thrown);
                }
            });

            awaitThreadWaiting(publisher);
            publisher.interrupt();
            publisher.join(2_000);

            assertFalse(publisher.isAlive());
            assertTrue(failure.get() instanceof InterruptedException);
            assertFalse(interruptedStatusInCatch.get(),
                    "抛 InterruptedException 时应遵守 JDK 约定清除中断状态");
        } finally {
            blocked.release();
            if (publisher != null) {
                publisher.interrupt();
                publisher.join(2_000);
            }
            terminate(pipeline);
        }
    }

    @Test
    void reportsNotRunningBeforeStartAndAfterNormalShutdown() throws Exception {
        DisruptorPipeline<TestEvent> pipeline = pipeline("not-running", 4,
                (event, sequence, endOfBatch) -> { });
        PipelineHandle<TestEvent> handle = pipeline.handle();
        try {
            assertEquals(PublicationResult.NOT_RUNNING, handle.tryPublishEvent(SET_VALUE, 1));
            assertEquals(PublicationResult.NOT_RUNNING,
                    handle.publishEvent(SET_VALUE, 1, TEST_TIMEOUT));

            start(pipeline);
            assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(SET_VALUE, 2));
            pipeline.requestShutdown(ShutdownMode.GRACEFUL, ShutdownDeadline.after(TEST_TIMEOUT));
            pipeline.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertEquals(PublicationResult.NOT_RUNNING, handle.tryPublishEvent(SET_VALUE, 3));
            assertEquals(PublicationResult.NOT_RUNNING,
                    handle.publishEvent(SET_VALUE, 3, TEST_TIMEOUT));
        } finally {
            terminate(pipeline);
        }
    }

    @RepeatedTest(5)
    void workerFailureWakesBoundedPublisherAndReportsPipelineFailure() throws Exception {
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch failHandler = new CountDownLatch(1);
        DisruptorPipeline<TestEvent> pipeline = pipeline("failure", 4,
                (event, sequence, endOfBatch) -> {
                    handlerEntered.countDown();
                    failHandler.await();
                    throw new IllegalStateException("consumer failed");
                });
        AtomicReference<PublicationResult> result = new AtomicReference<>();
        AtomicReference<Throwable> publisherFailure = new AtomicReference<>();
        Thread publisher = null;
        try {
            start(pipeline);
            for (int value = 0; value < 4; value++) {
                assertEquals(PublicationResult.PUBLISHED,
                        pipeline.handle().tryPublishEvent(SET_VALUE, value));
            }
            assertTrue(handlerEntered.await(2, TimeUnit.SECONDS));
            publisher = Thread.ofPlatform().name("failure-waiting-publisher").start(() -> {
                try {
                    result.set(pipeline.handle().publishEvent(
                            SET_VALUE, 99, Duration.ofSeconds(10)));
                } catch (Throwable thrown) {
                    publisherFailure.set(thrown);
                }
            });
            awaitThreadWaiting(publisher);

            failHandler.countDown();
            publisher.join(2_000);

            assertFalse(publisher.isAlive());
            assertNull(publisherFailure.get());
            assertEquals(PublicationResult.PIPELINE_FAILED, result.get());
        } finally {
            failHandler.countDown();
            if (publisher != null) {
                publisher.interrupt();
                publisher.join(2_000);
            }
            terminate(pipeline);
        }
    }

    @RepeatedTest(5)
    void shutdownWakesCapacityWaiterWithoutPublishing() throws Exception {
        BlockedConsumer blocked = new BlockedConsumer();
        DisruptorPipeline<TestEvent> pipeline = pipeline("shutdown-waiter", 4, blocked::onEvent);
        AtomicReference<PublicationResult> result = new AtomicReference<>();
        AtomicReference<Throwable> publisherFailure = new AtomicReference<>();
        Thread publisher = null;
        try {
            start(pipeline);
            fillRing(pipeline.handle(), blocked);
            publisher = Thread.ofPlatform().name("shutdown-waiting-publisher").start(() -> {
                try {
                    result.set(pipeline.handle().publishEvent(
                            SET_VALUE, 99, Duration.ofSeconds(10)));
                } catch (Throwable thrown) {
                    publisherFailure.set(thrown);
                }
            });
            awaitThreadWaiting(publisher);

            pipeline.requestShutdown(ShutdownMode.GRACEFUL, ShutdownDeadline.after(TEST_TIMEOUT));
            publisher.join(2_000);

            assertFalse(publisher.isAlive());
            assertNull(publisherFailure.get());
            assertEquals(PublicationResult.NOT_RUNNING, result.get());
        } finally {
            blocked.release();
            if (publisher != null) {
                publisher.interrupt();
                publisher.join(2_000);
            }
            terminate(pipeline);
        }
    }

    @Test
    void gracefulShutdownDrainsPublicationAdmittedBeforeGateClosed() throws Exception {
        CountDownLatch translatorEntered = new CountDownLatch(1);
        CountDownLatch releaseTranslator = new CountDownLatch(1);
        CountDownLatch consumed = new CountDownLatch(1);
        DisruptorPipeline<TestEvent> pipeline = pipeline("in-flight", 4,
                (event, sequence, endOfBatch) -> consumed.countDown());
        AtomicReference<PublicationResult> publication = new AtomicReference<>();
        AtomicReference<Throwable> publicationFailure = new AtomicReference<>();
        Thread publisher = null;
        try {
            start(pipeline);
            publisher = Thread.ofPlatform().name("in-flight-publisher").start(() -> {
                try {
                    publication.set(pipeline.handle().publishEvent((event, sequence) -> {
                        translatorEntered.countDown();
                        awaitUninterruptibly(releaseTranslator);
                        event.value = 42;
                    }, TEST_TIMEOUT));
                } catch (Throwable thrown) {
                    publicationFailure.set(thrown);
                }
            });
            assertTrue(translatorEntered.await(2, TimeUnit.SECONDS));

            pipeline.requestShutdown(ShutdownMode.GRACEFUL, ShutdownDeadline.after(TEST_TIMEOUT));
            assertFalse(pipeline.termination().toCompletableFuture().isDone());
            releaseTranslator.countDown();

            publisher.join(2_000);
            PipelineSnapshot snapshot = pipeline.termination().toCompletableFuture()
                    .get(2, TimeUnit.SECONDS);
            assertEquals(PublicationResult.PUBLISHED, publication.get());
            assertNull(publicationFailure.get());
            assertEquals(0L, consumed.getCount());
            assertTrue(snapshot.gracefulTermination());
        } finally {
            releaseTranslator.countDown();
            if (publisher != null) {
                publisher.interrupt();
                publisher.join(2_000);
            }
            terminate(pipeline);
        }
    }

    @Test
    void translatorFailurePropagatesAndStillReleasesPublisherAdmission() throws Exception {
        DisruptorPipeline<TestEvent> pipeline = pipeline("translator-failure", 4,
                (event, sequence, endOfBatch) -> { });
        RuntimeException original = new RuntimeException("translator failed");
        try {
            start(pipeline);

            RuntimeException thrown = assertThrows(RuntimeException.class,
                    () -> pipeline.handle().tryPublishEvent((event, sequence) -> {
                        throw original;
                    }));

            assertSame(original, thrown);
            pipeline.requestShutdown(ShutdownMode.GRACEFUL, ShutdownDeadline.after(TEST_TIMEOUT));
            assertTrue(pipeline.termination().toCompletableFuture()
                    .get(2, TimeUnit.SECONDS).gracefulTermination());
        } finally {
            terminate(pipeline);
        }
    }

    private static DisruptorPipeline<TestEvent> pipeline(
            String name,
            int bufferSize,
            com.lmax.disruptor.EventHandler<TestEvent> handler) {
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(name, TestEvent.class, TestEvent::new)
                .bufferSize(bufferSize)
                .threadFactory(runnable -> {
                    Thread thread = new Thread(runnable, "managed-publication-" + name);
                    thread.setUncaughtExceptionHandler((ignored, failure) -> { });
                    return thread;
                })
                .topology(disruptor -> disruptor.handleEventsWith(handler))
                .build();
        return ManagedPipeline.build(spec, spec.resolve(PipelineSettings.defaults()), TEST_TIMEOUT);
    }

    private static void start(DisruptorPipeline<?> pipeline) throws Exception {
        pipeline.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static void fillRing(PipelineHandle<TestEvent> handle, BlockedConsumer blocked)
            throws Exception {
        assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(SET_VALUE, 0));
        assertTrue(blocked.entered.await(2, TimeUnit.SECONDS));
        for (int value = 1; value < 4; value++) {
            assertEquals(PublicationResult.PUBLISHED, handle.tryPublishEvent(SET_VALUE, value));
        }
        assertEquals(0L, handle.remaining());
    }

    private static void awaitThreadWaiting(Thread thread) {
        long deadline = System.nanoTime() + TEST_TIMEOUT.toNanos();
        while (thread.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(Thread.State.TIMED_WAITING, thread.getState());
    }

    private static void terminate(DisruptorPipeline<?> pipeline) throws Exception {
        pipeline.requestShutdown(ShutdownMode.IMMEDIATE, ShutdownDeadline.after(TEST_TIMEOUT));
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

    private static final class BlockedConsumer {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
            entered.countDown();
            awaitUninterruptibly(release);
        }

        private void release() {
            release.countDown();
        }
    }

    private static final class TestEvent {
        private int value;
    }
}
