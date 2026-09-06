package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.concurrent.internal.EventLoopKernel;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import com.sstlfsj.disruptor.core.WorkerSupervisor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopGroupShutdownTest {

    @Test
    void startupFailureRollsBackEveryChildAndPreservesOriginalCause() throws Exception {
        IllegalStateException original = new IllegalStateException("child start failed");
        DisruptorEventLoopGroup group = EventLoopGroupBuilder.builder(
                        "startup-failure",
                        3,
                        (parent, childIndex) -> {
                            EventLoopBuilder<DisruptorEventLoop> builder =
                                    EventLoopBuilder.bounded("startup-failure-" + childIndex, 8);
                            if (childIndex == 1) {
                                builder.module(new EventLoopModule() {
                                    @Override
                                    public void onStart(EventLoop loop) {
                                        throw original;
                                    }
                                });
                            }
                            return builder.build();
                        })
                .shutdownTimeout(Duration.ofSeconds(2))
                .build();

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> group.start().toCompletableFuture().get(2, TimeUnit.SECONDS));
        assertSame(original, failure.getCause());
        EventLoopGroupSnapshot terminated = group.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertEquals(SupervisedLifecycle.TERMINATED, terminated.lifecycle());
        assertSame(original, terminated.failure());
        assertFalse(terminated.acceptingTasks());
        assertTrue(terminated.children().stream()
                .allMatch(child -> child.lifecycle() == SupervisedLifecycle.TERMINATED));
        ShutdownDeadline rollbackDeadline = null;
        for (EventLoop child : group) {
            ShutdownDeadline actual = supervisorDeadline(child);
            assertTrue(actual.isBounded());
            if (rollbackDeadline == null) {
                rollbackDeadline = actual;
            } else {
                assertSame(rollbackDeadline, actual);
            }
        }
    }

    @Test
    void threadStartFailureAlsoFreezesTheGroupRollbackDeadlineFirst() throws Exception {
        IllegalStateException original = new IllegalStateException("thread start failed");
        DisruptorEventLoopGroup group = EventLoopGroupBuilder.builder(
                        "thread-start-failure",
                        3,
                        (parent, childIndex) -> {
                            EventLoopBuilder<DisruptorEventLoop> builder = EventLoopBuilder
                                    .bounded("thread-start-failure-" + childIndex, 8);
                            if (childIndex == 1) {
                                builder.threadFactory(command -> new Thread(command) {
                                    @Override
                                    public synchronized void start() {
                                        throw original;
                                    }
                                });
                            }
                            return builder.build();
                        })
                .shutdownTimeout(Duration.ofSeconds(2))
                .build();

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> group.start().toCompletableFuture().get(2, TimeUnit.SECONDS));
        assertSame(original, failure.getCause());
        group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);

        ShutdownDeadline rollbackDeadline = null;
        for (EventLoop child : group) {
            ShutdownDeadline actual = supervisorDeadline(child);
            assertTrue(actual.isBounded());
            if (rollbackDeadline == null) {
                rollbackDeadline = actual;
            } else {
                assertSame(rollbackDeadline, actual);
            }
        }
    }

    @Test
    void explicitAndStandardShutdownBroadcastTheSameDeadlineObject() throws Exception {
        DisruptorEventLoopGroup explicit = EventLoopGroupBuilder
                .bounded("shared-explicit", 3, 8)
                .build();
        explicit.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        ShutdownDeadline deadline = ShutdownDeadline.after(Duration.ofSeconds(2));

        explicit.requestShutdown(ShutdownMode.GRACEFUL, deadline);

        for (EventLoop child : explicit) {
            assertSame(deadline, supervisorDeadline(child));
        }
        explicit.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);

        DisruptorEventLoopGroup standard = EventLoopGroupBuilder
                .bounded("shared-standard", 2, 8)
                .build();
        standard.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        standard.shutdown();
        ShutdownDeadline first = null;
        for (EventLoop child : standard) {
            ShutdownDeadline actual = supervisorDeadline(child);
            assertFalse(actual.isBounded());
            if (first == null) {
                first = actual;
            } else {
                assertSame(first, actual);
            }
        }
        standard.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void shutdownNowAggregatesByChildIndexThenChildAcceptedSequence() throws Exception {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("group-now", 2, 8)
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        List<EventLoop> children = children(group);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        children.forEach(child -> child.execute(() -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        }));
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        NamedRunnable child0First = new NamedRunnable("child-0-first");
        NamedRunnable child0Second = new NamedRunnable("child-0-second");
        NamedRunnable child1First = new NamedRunnable("child-1-first");
        children.get(1).execute(child1First);
        children.get(0).execute(child0First);
        children.get(0).execute(child0Second);

        List<Runnable> returned = group.shutdownNow();

        assertEquals(List.of(child0First, child0Second, child1First), returned);
        assertFalse(group.termination().toCompletableFuture().isDone());
        release.countDown();
        group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void groupSnapshotAggregatesDiscardedTasksFromEveryChild() throws Exception {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("group-discarded", 2, 8)
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        List<EventLoop> children = children(group);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try {
            for (EventLoop child : children) {
                child.execute(() -> {
                    entered.countDown();
                    awaitIgnoringInterrupt(release);
                });
            }
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            List<Future<?>> queued = new ArrayList<>(children.size());
            for (EventLoop child : children) {
                queued.add(child.submit(() -> { }));
            }

            group.requestShutdown(ShutdownMode.IMMEDIATE, ShutdownDeadline.unbounded());
            awaitCondition(() -> queued.stream().allMatch(Future::isCancelled));
            release.countDown();
            EventLoopGroupSnapshot terminated = group.termination().toCompletableFuture()
                    .get(2, TimeUnit.SECONDS);

            assertEquals(2, terminated.discardedTasks());
            assertEquals(2, terminated.children().stream()
                    .mapToLong(EventLoopSnapshot::discardedTasks)
                    .sum());
        } finally {
            release.countDown();
            if (!group.isTerminated()) {
                group.shutdownNow();
                group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shutdownNowFreezesEveryAcceptedChildPublicationBeforeRegistrySweep()
            throws Exception {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("group-admission-freeze", 1, 8192)
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        EventLoop child = group.select(0);
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        child.execute(() -> {
            workerEntered.countDown();
            awaitIgnoringInterrupt(releaseWorker);
        });
        assertTrue(workerEntered.await(2, TimeUnit.SECONDS));

        ExecutorService producers = Executors.newFixedThreadPool(4);
        Queue<NamedRunnable> accepted = new ConcurrentLinkedQueue<>();
        AtomicInteger ids = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> producerResults = new ArrayList<>();
        try {
            for (int producer = 0; producer < 4; producer++) {
                producerResults.add(producers.submit(() -> {
                    start.await();
                    while (true) {
                        NamedRunnable task = new NamedRunnable(
                                "racing-" + ids.getAndIncrement());
                        if (!child.tryExecute(task)) {
                            return null;
                        }
                        accepted.add(task);
                    }
                }));
            }
            start.countDown();
            while (accepted.size() < 100) {
                Thread.onSpinWait();
            }

            List<Runnable> returned = group.shutdownNow();
            for (Future<?> producerResult : producerResults) {
                producerResult.get(2, TimeUnit.SECONDS);
            }

            assertEquals(accepted.size(), returned.size());
            assertEquals(new HashSet<>(accepted), new HashSet<>(returned));
            assertEquals(returned.size(), new HashSet<>(returned).size());
        } finally {
            producers.shutdownNow();
            releaseWorker.countDown();
            group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void groupClosesEveryChildAndDrainsPublishersBeforeBroadcastingShutdown()
            throws Exception {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("group-two-phase-close", 2, 8)
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        List<EventLoop> children = children(group);
        Object activeGate = admissionGate(children.get(0));
        assertTrue(invokeGateBoolean(activeGate, "tryEnter"));

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> shutdown = caller.submit(group::shutdown);
            try {
                awaitCondition(() -> !group.snapshot().acceptingTasks());
                awaitChildGatesClosed(children);

                assertFalse(shutdown.isDone(), "Group 必须等待已进入 child gate 的 publisher");
                for (EventLoop child : children) {
                    assertFalse(invokeGateBoolean(admissionGate(child), "isAccepting"));
                    assertNull(supervisorDeadline(child),
                            "全部 child drain 前不能广播 supervisor shutdown");
                }
            } finally {
                if (invokeGateLong(activeGate, "activePublishers") != 0) {
                    invokeGateLeave(activeGate, true);
                }
            }
            shutdown.get(2, TimeUnit.SECONDS);
        } finally {
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(2, TimeUnit.SECONDS));
        }
        group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void childInfrastructureFailureFailStopsGroupWithoutRemappingAffinity() throws Exception {
        IllegalStateException original = new IllegalStateException("module update failed");
        DisruptorEventLoopGroup group = EventLoopGroupBuilder.builder(
                        "child-failure",
                        2,
                        (parent, childIndex) -> EventLoopBuilder
                                .bounded("child-failure-" + childIndex, 8)
                                .module(childIndex == 0 ? new EventLoopModule() {
                                    @Override
                                    public void onUpdate(EventLoop loop, long nowNanos) {
                                        throw original;
                                    }
                                } : new EventLoopModule() { })
                                .build())
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        EventLoop selected = group.select(Integer.MIN_VALUE);

        group.iterator().next().execute(() -> { });
        EventLoopGroupSnapshot terminated = group.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertSame(selected, group.select(Integer.MIN_VALUE));
        assertSame(original, terminated.failure());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertTrue(terminated.children().stream()
                .allMatch(child -> child.lifecycle() == SupervisedLifecycle.TERMINATED));
    }

    @Test
    void childFailureObservedDuringGracefulQuiesceStillUpgradesWholeGroup() throws Exception {
        IllegalStateException original = new IllegalStateException("late module failure");
        CountDownLatch updateEntered = new CountDownLatch(1);
        CountDownLatch releaseUpdate = new CountDownLatch(1);
        DisruptorEventLoopGroup group = EventLoopGroupBuilder.builder(
                        "quiescing-child-failure",
                        2,
                        (parent, childIndex) -> EventLoopBuilder
                                .bounded("quiescing-child-failure-" + childIndex, 8)
                                .module(childIndex == 0 ? new EventLoopModule() {
                                    @Override
                                    public void onUpdate(EventLoop loop, long nowNanos)
                                            throws Exception {
                                        updateEntered.countDown();
                                        releaseUpdate.await();
                                        throw original;
                                    }
                                } : new EventLoopModule() { })
                                .build())
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        group.select(0).execute(() -> { });
        assertTrue(updateEntered.await(2, TimeUnit.SECONDS));

        group.shutdown();
        assertEquals(SupervisedLifecycle.QUIESCING, group.snapshot().lifecycle());
        releaseUpdate.countDown();
        EventLoopGroupSnapshot terminated = group.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertSame(original, terminated.failure());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertTrue(terminated.children().stream()
                .allMatch(child -> child.lifecycle() == SupervisedLifecycle.TERMINATED));
    }

    @Test
    void groupTerminationWaitsForLastInterruptIgnoringChild() throws Exception {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("group-real-termination", 2, 8)
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        group.select(1).execute(() -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        group.shutdownNow();
        Thread.sleep(50);

        assertFalse(group.termination().toCompletableFuture().isDone());
        release.countDown();
        EventLoopGroupSnapshot terminated = group.termination().toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        assertEquals(SupervisedLifecycle.TERMINATED, terminated.lifecycle());
        assertEquals(0, terminated.executingTasks());
    }

    private static ShutdownDeadline supervisorDeadline(EventLoop child) throws Exception {
        Object kernel = fieldValue(child, EventLoopKernel.class);
        WorkerSupervisor supervisor = (WorkerSupervisor) fieldValue(kernel, WorkerSupervisor.class);
        Field deadline = WorkerSupervisor.class.getDeclaredField("shutdownDeadline");
        deadline.setAccessible(true);
        return (ShutdownDeadline) deadline.get(supervisor);
    }

    private static Object admissionGate(EventLoop child) throws Exception {
        Object kernel = fieldValue(child, EventLoopKernel.class);
        Field gate = EventLoopKernel.class.getDeclaredField("gate");
        gate.setAccessible(true);
        return gate.get(kernel);
    }

    private static boolean invokeGateBoolean(Object gate, String method) throws Exception {
        var operation = gate.getClass().getDeclaredMethod(method);
        operation.setAccessible(true);
        return (boolean) operation.invoke(gate);
    }

    private static long invokeGateLong(Object gate, String method) throws Exception {
        var operation = gate.getClass().getDeclaredMethod(method);
        operation.setAccessible(true);
        return (long) operation.invoke(gate);
    }

    private static void invokeGateLeave(Object gate, boolean rollback) throws Exception {
        var operation = gate.getClass().getDeclaredMethod("leave", boolean.class);
        operation.setAccessible(true);
        operation.invoke(gate, rollback);
    }

    private static void awaitChildGatesClosed(List<EventLoop> children) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (true) {
            boolean allClosed = true;
            for (EventLoop child : children) {
                if (invokeGateBoolean(admissionGate(child), "isAccepting")) {
                    allClosed = false;
                    break;
                }
            }
            if (allClosed) {
                return;
            }
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("等待 child gate 关闭超时");
            }
            Thread.sleep(1);
        }
    }

    private static Object fieldValue(Object owner, Class<?> fieldType) throws Exception {
        Class<?> type = owner.getClass();
        while (type != null) {
            for (Field field : type.getDeclaredFields()) {
                if (field.getType() == fieldType) {
                    field.setAccessible(true);
                    return field.get(owner);
                }
            }
            type = type.getSuperclass();
        }
        throw new AssertionError("未找到字段：" + fieldType.getName());
    }

    private static List<EventLoop> children(EventLoopGroup group) {
        List<EventLoop> children = new ArrayList<>();
        group.forEach(children::add);
        return List.copyOf(children);
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

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        while (latch.getCount() != 0) {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                // 真实退出仍由测试控制。
            }
        }
    }

    private record NamedRunnable(String name) implements Runnable {
        @Override
        public void run() {
        }
    }
}
