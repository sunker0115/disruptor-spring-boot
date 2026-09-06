package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.concurrent.internal.GroupLifecycleCoordinator;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** 固定 child 集合及其完整生命周期的唯一 owner。 */
public final class DisruptorEventLoopGroup
        extends AbstractExecutorService implements EventLoopGroup {

    private final String name;
    private final List<EventLoop> children;
    private final GroupLifecycleCoordinator coordinator;
    private final AtomicInteger nextChild = new AtomicInteger();
    private final CompletableFuture<EventLoopGroupSnapshot> termination =
            new CompletableFuture<>();
    private final CompletionStage<EventLoopGroupSnapshot> terminationView =
            termination.minimalCompletionStage();

    DisruptorEventLoopGroup(
            String name,
            int childCount,
            EventLoopFactory eventLoopFactory,
            Duration shutdownTimeout) {
        this.name = Objects.requireNonNull(name, "name 不能为空");
        Objects.requireNonNull(eventLoopFactory, "eventLoopFactory 不能为空");
        this.coordinator = new GroupLifecycleCoordinator(name, shutdownTimeout);

        Object lifecycleOwnerToken = new Object();
        List<EventLoop> createdChildren = new ArrayList<>(childCount);
        List<GroupLifecycleCoordinator.ChildControl> controls = new ArrayList<>(childCount);
        Set<EventLoop> identities = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int childIndex = 0; childIndex < childCount; childIndex++) {
            EventLoop created = Objects.requireNonNull(
                    eventLoopFactory.create(this, childIndex),
                    "eventLoopFactory 不能返回 null：childIndex=" + childIndex);
            if (!identities.add(created)) {
                throw new IllegalArgumentException(
                        "eventLoopFactory 不能复用 child 实例：childIndex=" + childIndex);
            }
            if (!(created instanceof AbstractEventLoop child)) {
                throw new IllegalArgumentException(
                        "Group child 必须由 EventLoopBuilder 创建：type="
                                + created.getClass().getName());
            }
            child.bindOwner(this, childIndex, coordinator, lifecycleOwnerToken);
            createdChildren.add(child);
            controls.add(new GroupLifecycleCoordinator.ChildControl(
                    child,
                    () -> child.startFromOwner(lifecycleOwnerToken),
                    new GroupLifecycleCoordinator.ChildAdmissions() {
                        @Override
                        public void close() {
                            child.closeAdmissionsFromOwner(lifecycleOwnerToken);
                        }

                        @Override
                        public void awaitDrained() {
                            child.awaitAdmissionsDrainedFromOwner(lifecycleOwnerToken);
                        }
                    },
                    (mode, deadline) -> child.requestShutdownFromOwner(
                            lifecycleOwnerToken, mode, deadline),
                    deadline -> child.shutdownNowFromOwner(
                            lifecycleOwnerToken, deadline)));
        }
        this.children = List.copyOf(createdChildren);
        coordinator.bindChildren(controls);
        coordinator.termination().whenComplete((ignored, failure) ->
                Thread.ofVirtual().name(name + "-group-result-notifier").start(() -> {
                    if (failure != null) {
                        termination.completeExceptionally(unwrap(failure));
                    } else {
                        termination.complete(snapshot());
                    }
                }));
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CompletionStage<Void> start() {
        return coordinator.start();
    }

    @Override
    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        coordinator.requestShutdown(mode, deadline);
    }

    @Override
    public CompletionStage<EventLoopGroupSnapshot> termination() {
        return terminationView;
    }

    @Override
    public EventLoopGroupSnapshot snapshot() {
        List<EventLoopSnapshot> childSnapshots = children.stream()
                .map(EventLoop::snapshot)
                .toList();
        GroupLifecycleCoordinator.StateSnapshot state = coordinator.snapshot();
        return EventLoopGroupSnapshot.builder()
                .name(name)
                .lifecycle(state.lifecycle())
                .acceptingTasks(state.accepting())
                .childCount(childSnapshots.size())
                .outstandingTasks(sum(childSnapshots, EventLoopSnapshot::outstandingTasks))
                .executingTasks(sum(childSnapshots, EventLoopSnapshot::executingTasks))
                .completedTasks(sum(childSnapshots, EventLoopSnapshot::completedTasks))
                .failedTasks(sum(childSnapshots, EventLoopSnapshot::failedTasks))
                .cancelledTasks(sum(childSnapshots, EventLoopSnapshot::cancelledTasks))
                .shutdownNowReturnedTasks(sum(
                        childSnapshots, EventLoopSnapshot::shutdownNowReturnedTasks))
                .failure(state.failure())
                .shutdownMode(state.shutdownMode())
                .children(childSnapshots)
                .build();
    }

    @Override
    public EventLoop next() {
        return children.get(Math.floorMod(nextChild.getAndIncrement(), children.size()));
    }

    @Override
    public EventLoop select(int affinityKey) {
        return children.get(Math.floorMod(affinityKey, children.size()));
    }

    @Override
    public Iterator<EventLoop> iterator() {
        return children.iterator();
    }

    @Override
    public <V> EventLoopScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec) {
        return next().schedule(spec);
    }

    @Override
    public void execute(Runnable command) {
        next().execute(command);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return next().submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return next().submit(task, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return next().submit(task);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return next().schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return next().schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit) {
        return next().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command,
            long initialDelay,
            long delay,
            TimeUnit unit) {
        return next().scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        coordinator.requestShutdown(ShutdownMode.GRACEFUL, ShutdownDeadline.unbounded());
    }

    @Override
    public List<Runnable> shutdownNow() {
        return coordinator.shutdownNow(ShutdownDeadline.unbounded());
    }

    @Override
    public boolean isShutdown() {
        SupervisedLifecycle lifecycle = coordinator.snapshot().lifecycle();
        return lifecycle == SupervisedLifecycle.QUIESCING
                || lifecycle == SupervisedLifecycle.STOPPING
                || lifecycle == SupervisedLifecycle.TERMINATED;
    }

    @Override
    public boolean isTerminated() {
        return coordinator.snapshot().lifecycle() == SupervisedLifecycle.TERMINATED;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit 不能为空");
        rejectChildBlocking("不能在所属 Group child 线程等待 Group 终止");
        try {
            termination.toCompletableFuture().get(Math.max(0L, timeout), unit);
            return true;
        } catch (TimeoutException ignored) {
            return false;
        } catch (ExecutionException failure) {
            throw new CompletionException(failure.getCause());
        }
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        rejectBlockingInvoke(tasks);
        return super.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit) throws InterruptedException {
        rejectBlockingInvoke(tasks);
        return super.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        rejectBlockingInvoke(tasks);
        return super.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        rejectBlockingInvoke(tasks);
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void close() {
        rejectChildBlocking("不能在所属 Group child 线程关闭并等待 Group");
        shutdown();
        boolean interrupted = false;
        while (true) {
            try {
                termination.toCompletableFuture().get();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
                shutdownNow();
            } catch (ExecutionException failure) {
                throw new CompletionException(failure.getCause());
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void rejectBlockingInvoke(Collection<?> tasks) {
        Objects.requireNonNull(tasks, "tasks 不能为空");
        if (!tasks.isEmpty()) {
            rejectChildBlocking("不能在所属 Group child 线程执行阻塞式批量调用");
        }
    }

    private void rejectChildBlocking(String message) {
        if (children.stream().anyMatch(EventLoop::inEventLoop)) {
            throw new BlockingOperationException(message);
        }
    }

    private static long sum(
            List<EventLoopSnapshot> snapshots,
            SnapshotCounter counter) {
        long total = 0L;
        for (EventLoopSnapshot snapshot : snapshots) {
            total += counter.value(snapshot);
        }
        return total;
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    @FunctionalInterface
    private interface SnapshotCounter {
        long value(EventLoopSnapshot snapshot);
    }
}
