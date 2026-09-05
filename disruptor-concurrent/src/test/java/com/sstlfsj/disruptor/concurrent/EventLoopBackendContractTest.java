package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.concurrent.internal.EventLoopKernel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopBackendContractTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void bothBackendsShareStartupExecutionSchedulingCancellationAndShutdown(
            String backend,
            LoopFactory factory) throws Exception {
        java.util.ArrayList<String> moduleEvents = new java.util.ArrayList<>();
        EventLoopModule module = new EventLoopModule() {
            @Override
            public void onStart(EventLoop loop) {
                moduleEvents.add("start");
            }

            @Override
            public void onStop(EventLoop loop) {
                moduleEvents.add("stop");
            }
        };
        EventLoop loop = factory.create(backend + "-contract", module);

        assertFalse(loop.tryExecute(() -> { }));
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        Future<String> submitted = loop.submit(() -> "value");
        ScheduledFuture<String> scheduled = loop.schedule(() -> "scheduled", 0,
                TimeUnit.NANOSECONDS);
        EventLoopScheduledFuture<Void> cancelled = loop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .triggerAfter(Duration.ofDays(1))
                .build());

        assertEquals("value", submitted.get(2, TimeUnit.SECONDS));
        assertEquals("scheduled", scheduled.get(2, TimeUnit.SECONDS));
        assertTrue(cancelled.cancel(false));
        awaitCondition(() -> loop.snapshot().outstandingTasks() == 0);
        loop.shutdown();
        EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertTrue(terminated.worker().gracefulTermination());
        assertEquals(0, terminated.outstandingTasks());
        assertEquals(List.of("start", "stop"), moduleEvents);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void bothBackendsRemainLosslessWithConcurrentProducers(
            String backend,
            LoopFactory factory) throws Exception {
        EventLoop loop = factory.create(backend + "-producers");
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        int producers = 4;
        int tasksPerProducer = 1_000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(producers * tasksPerProducer);
        java.util.concurrent.atomic.AtomicInteger duplicates = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicIntegerArray seen =
                new java.util.concurrent.atomic.AtomicIntegerArray(producers * tasksPerProducer);
        Thread[] threads = new Thread[producers];
        for (int producer = 0; producer < producers; producer++) {
            int producerIndex = producer;
            threads[producer] = Thread.ofPlatform().start(() -> {
                await(start);
                for (int index = 0; index < tasksPerProducer; index++) {
                    int taskIndex = producerIndex * tasksPerProducer + index;
                    while (!loop.tryExecute(() -> {
                        if (!seen.compareAndSet(taskIndex, 0, 1)) {
                            duplicates.incrementAndGet();
                        }
                        completed.countDown();
                    })) {
                        Thread.onSpinWait();
                    }
                }
            });
        }
        start.countDown();

        assertTrue(completed.await(10, TimeUnit.SECONDS));
        for (Thread thread : threads) {
            thread.join(2_000);
            assertFalse(thread.isAlive());
        }
        assertEquals(0, duplicates.get());
        for (int index = 0; index < seen.length(); index++) {
            assertEquals(1, seen.get(index));
        }
        loop.shutdown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void bothBackendsImplementAllFourSchedulingModes(
            String backend,
            LoopFactory factory) throws Exception {
        EventLoop loop = factory.create(backend + "-scheduling");
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertEquals("one-shot", loop.schedule(() -> "one-shot", 0, TimeUnit.NANOSECONDS)
                .get(2, TimeUnit.SECONDS));
        EventLoopScheduledFuture<Void> fixedRate = loop.schedule(periodicSpec(
                ScheduleMode.FIXED_RATE, Duration.ofMillis(1), null));
        EventLoopScheduledFuture<Void> fixedDelay = loop.schedule(periodicSpec(
                ScheduleMode.FIXED_DELAY, Duration.ofMillis(1), null));
        EventLoopScheduledFuture<Void> dynamic = loop.schedule(periodicSpec(
                ScheduleMode.DYNAMIC_DELAY, null, lastRun -> Duration.ofMillis(1)));

        awaitCondition(() -> fixedRate.isDone() && fixedDelay.isDone() && dynamic.isDone());
        assertEquals(2, fixedRate.snapshot().executions());
        assertEquals(2, fixedDelay.snapshot().executions());
        assertEquals(2, dynamic.snapshot().executions());
        assertEquals(CancellationReason.MAX_EXECUTIONS,
                fixedRate.snapshot().cancellationReason());
        loop.shutdown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void bothBackendsGuardBlockingAndWaitForRealWorkerExit(
            String backend,
            LoopFactory factory) throws Exception {
        EventLoop loop = factory.create(backend + "-blocking");
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        AtomicReference<Throwable> blocked = new AtomicReference<>();
        CountDownLatch guardChecked = new CountDownLatch(1);
        loop.execute(() -> {
            Future<?> pending = loop.submit(() -> { });
            try {
                pending.get();
            } catch (Throwable failure) {
                blocked.set(failure);
            } finally {
                guardChecked.countDown();
            }
        });
        assertTrue(guardChecked.await(2, TimeUnit.SECONDS));
        assertInstanceOf(BlockingOperationException.class, blocked.get());

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        loop.execute(() -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        loop.shutdownNow();
        Thread.sleep(30);
        assertFalse(loop.termination().toCompletableFuture().isDone());
        release.countDown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    void bothBackendsReturnWaitingOriginalCommandsInAcceptedOrder(
            String backend,
            LoopFactory factory) throws Exception {
        EventLoop loop = factory.create(backend + "-shutdown-now");
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        loop.execute(() -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        NamedRunnable first = new NamedRunnable("first");
        NamedRunnable third = new NamedRunnable("third");
        loop.execute(first);
        Future<String> second = loop.submit(() -> "second");
        loop.schedule(third, 1, TimeUnit.DAYS);

        List<Runnable> returned = loop.shutdownNow();

        assertEquals(3, returned.size());
        assertSame(first, returned.get(0));
        assertSame(second, returned.get(1));
        assertSame(third, returned.get(2));
        release.countDown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @org.junit.jupiter.api.Test
    void facadesHoldTheSameKernelImplementationClass() throws Exception {
        EventLoop bounded = EventLoopBuilder.bounded("bounded-kernel", 8).build();
        EventLoop unbounded = EventLoopBuilder.unbounded("unbounded-kernel", 8).build();

        Object boundedKernel = findKernel(bounded);
        Object unboundedKernel = findKernel(unbounded);

        assertSame(EventLoopKernel.class, boundedKernel.getClass());
        assertSame(EventLoopKernel.class, unboundedKernel.getClass());
        bounded.shutdownNow();
        unbounded.shutdownNow();
    }

    @org.junit.jupiter.api.Test
    void ordinaryOwnershipDoesNotAddAVolatileRunnableOutsideTheQueueSlot() {
        long owners = java.util.Arrays.stream(EventLoopKernel.class.getDeclaredFields())
                .filter(field -> field.getType() == Runnable.class)
                .filter(field -> java.lang.reflect.Modifier.isVolatile(field.getModifiers()))
                .count();

        assertEquals(0, owners, "ordinary 的物理所有权只保留在槽位中");
    }

    static Stream<Arguments> backends() {
        return Stream.of(
                Arguments.of("bounded", (LoopFactory) (name, modules) -> {
                    EventLoopBuilder<DisruptorEventLoop> builder =
                            EventLoopBuilder.bounded(name, 8_192);
                    for (EventLoopModule module : modules) {
                        builder.module(module);
                    }
                    return builder.build();
                }),
                Arguments.of("unbounded", (LoopFactory) (name, modules) -> {
                    EventLoopBuilder<UnboundedEventLoop> builder =
                            EventLoopBuilder.unbounded(name, 64);
                    for (EventLoopModule module : modules) {
                        builder.module(module);
                    }
                    return builder.build();
                }));
    }

    private static Object findKernel(EventLoop loop) throws ReflectiveOperationException {
        Class<?> type = loop.getClass();
        while (type != null) {
            for (Field field : type.getDeclaredFields()) {
                if (field.getType() == EventLoopKernel.class) {
                    field.setAccessible(true);
                    return field.get(loop);
                }
            }
            type = type.getSuperclass();
        }
        throw new AssertionError("门面未持有 EventLoopKernel");
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
        while (latch.getCount() != 0) {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                // shutdownNow 只请求中断，真实退出仍由测试控制。
            }
        }
    }

    private static ScheduledTaskSpec<Void> periodicSpec(
            ScheduleMode mode,
            Duration period,
            DynamicDelay dynamicDelay) {
        ScheduledTaskSpec.ScheduledTaskSpecBuilder<Void> builder =
                ScheduledTaskSpec.<Void>builder()
                        .task(context -> null)
                        .scheduleMode(mode)
                        .maxExecutions(2);
        if (period != null) {
            builder.period(period);
        }
        if (dynamicDelay != null) {
            builder.dynamicDelay(dynamicDelay);
        }
        return builder.build();
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

    @FunctionalInterface
    private interface LoopFactory {
        EventLoop create(String name, EventLoopModule... modules);
    }

    private record NamedRunnable(String name) implements Runnable {
        @Override
        public void run() {
        }
    }
}
