package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** 聚合管理 Spring 容器中显式声明的根 EventLoop 与 EventLoopGroup。 */
public final class DisruptorConcurrentLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(
            DisruptorConcurrentLifecycle.class);

    private final List<SupervisedScheduledExecutor<?>> managedExecutors;
    private final int phase;
    private final Duration shutdownTimeout;
    private final CompletableFuture<Void> startup = new CompletableFuture<>();
    private final CompletableFuture<Void> stopCompletion = new CompletableFuture<>();
    private final AtomicBoolean startRequested = new AtomicBoolean();
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();

    public DisruptorConcurrentLifecycle(
            List<SupervisedScheduledExecutor<?>> candidates,
            int phase,
            Duration shutdownTimeout) {
        this.managedExecutors = selectRoots(candidates);
        if (managedExecutors.isEmpty()) {
            throw new IllegalArgumentException("至少需要一个根 EventLoop 或 EventLoopGroup");
        }
        this.phase = phase;
        this.shutdownTimeout = requirePositive(shutdownTimeout);
    }

    @Override
    public void start() {
        if (startRequested.compareAndSet(false, true)) {
            startFirstCaller();
            return;
        }
        awaitStartup();
    }

    @Override
    public void stop() {
        CountDownLatch stopped = new CountDownLatch(1);
        stop(stopped::countDown);
        boolean interrupted = false;
        while (true) {
            try {
                stopped.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        stopCompletion.whenComplete((ignored, failure) -> callback.run());
        if (!stopRequested.compareAndSet(false, true)) {
            return;
        }

        ShutdownDeadline deadline = ShutdownDeadline.after(shutdownTimeout);
        List<Throwable> requestFailures = requestAll(
                ShutdownMode.GRACEFUL, deadline, null);
        Thread.ofVirtual().name("disruptor-concurrent-spring-stop")
                .start(() -> {
                    List<Throwable> failures = awaitTerminations(requestFailures);
                    running.set(false);
                    logFailures("停止 EventLoop 根对象时发生异常", failures);
                    stopCompletion.complete(null);
                });
    }

    @Override
    public boolean isRunning() {
        return running.get() && managedExecutors.stream().noneMatch(
                SupervisedScheduledExecutor::isShutdown);
    }

    @Override
    public boolean isPauseable() {
        return false;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    List<SupervisedScheduledExecutor<?>> managedExecutors() {
        return managedExecutors;
    }

    private void startFirstCaller() {
        List<CompletableFuture<Void>> stages = new ArrayList<>(managedExecutors.size());
        for (SupervisedScheduledExecutor<?> executor : managedExecutors) {
            try {
                CompletionStage<Void> stage = Objects.requireNonNull(
                        executor.start(), "start stage 不能为空：" + executor.name());
                stages.add(stage.toCompletableFuture());
            } catch (Throwable failure) {
                stages.add(CompletableFuture.failedFuture(failure));
            }
        }
        awaitAll(stages);
        Throwable failure = collectStageFailures(stages);
        if (failure != null) {
            ShutdownDeadline deadline = ShutdownDeadline.after(shutdownTimeout);
            List<Throwable> rollbackFailures = requestAll(
                    ShutdownMode.IMMEDIATE, deadline, failure);
            suppressInto(failure, awaitTerminations(rollbackFailures));
            startup.completeExceptionally(failure);
            sneakyThrow(failure);
            return;
        }
        if (stopRequested.get()) {
            Throwable aborted = new IllegalStateException(
                    "EventLoop 根对象启动完成前已开始关闭");
            startup.completeExceptionally(aborted);
            sneakyThrow(aborted);
            return;
        }
        running.set(true);
        startup.complete(null);
    }

    private void awaitStartup() {
        try {
            startup.join();
        } catch (CompletionException failure) {
            sneakyThrow(unwrap(failure));
        }
    }

    private List<Throwable> requestAll(
            ShutdownMode mode,
            ShutdownDeadline deadline,
            Throwable primary) {
        List<Throwable> failures = new ArrayList<>();
        if (primary != null) {
            failures.add(primary);
        }
        for (SupervisedScheduledExecutor<?> executor : managedExecutors) {
            try {
                executor.requestShutdown(mode, deadline);
            } catch (Throwable failure) {
                addFailure(failures, failure);
            }
        }
        return failures;
    }

    private List<Throwable> awaitTerminations(List<Throwable> failures) {
        List<CompletableFuture<?>> stages = new ArrayList<>(managedExecutors.size());
        for (SupervisedScheduledExecutor<?> executor : managedExecutors) {
            try {
                CompletionStage<?> stage = Objects.requireNonNull(
                        executor.termination(), "termination stage 不能为空：" + executor.name());
                stages.add(stage.toCompletableFuture());
            } catch (Throwable failure) {
                addFailure(failures, failure);
            }
        }
        awaitAll(stages);
        for (CompletableFuture<?> stage : stages) {
            try {
                stage.join();
            } catch (Throwable failure) {
                addFailure(failures, unwrap(failure));
            }
        }
        return failures;
    }

    private static Throwable collectStageFailures(List<CompletableFuture<Void>> stages) {
        List<Throwable> failures = new ArrayList<>();
        for (CompletableFuture<Void> stage : stages) {
            try {
                stage.join();
            } catch (Throwable failure) {
                addFailure(failures, unwrap(failure));
            }
        }
        if (failures.isEmpty()) {
            return null;
        }
        Throwable primary = failures.get(0);
        for (int index = 1; index < failures.size(); index++) {
            Throwable next = failures.get(index);
            if (primary != next && !containsIdentity(primary.getSuppressed(), next)) {
                primary.addSuppressed(next);
            }
        }
        return primary;
    }

    private static void suppressInto(Throwable primary, List<Throwable> failures) {
        for (Throwable failure : failures) {
            if (failure != primary && !containsIdentity(primary.getSuppressed(), failure)) {
                primary.addSuppressed(failure);
            }
        }
    }

    private static void awaitAll(List<? extends CompletableFuture<?>> stages) {
        try {
            CompletableFuture.allOf(stages.toArray(CompletableFuture[]::new)).join();
        } catch (CompletionException ignored) {
            // 完成后逐项收集精确根对象失败。
        }
    }

    private static List<SupervisedScheduledExecutor<?>> selectRoots(
            List<SupervisedScheduledExecutor<?>> candidates) {
        Objects.requireNonNull(candidates, "candidates 不能为空");
        List<SupervisedScheduledExecutor<?>> roots = new ArrayList<>();
        Set<SupervisedScheduledExecutor<?>> identities = Collections.newSetFromMap(
                new IdentityHashMap<>());
        for (SupervisedScheduledExecutor<?> candidate : candidates) {
            Objects.requireNonNull(candidate, "candidate 不能为空");
            if (candidate instanceof EventLoop loop && loop.parent() != null) {
                continue;
            }
            if (identities.add(candidate)) {
                roots.add(candidate);
            }
        }
        return List.copyOf(roots);
    }

    private static Duration requirePositive(Duration value) {
        Objects.requireNonNull(value, "shutdownTimeout 不能为空");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(
                    "shutdownTimeout 必须为正数，实际值=" + value);
        }
        value.toNanos();
        return value;
    }

    private static void addFailure(List<Throwable> failures, Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        if (failures.stream().noneMatch(existing -> existing == unwrapped)) {
            failures.add(unwrapped);
        }
    }

    private static boolean containsIdentity(Throwable[] failures, Throwable expected) {
        for (Throwable failure : failures) {
            if (failure == expected) {
                return true;
            }
        }
        return false;
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private static void logFailures(String message, List<Throwable> failures) {
        for (Throwable failure : failures) {
            log.warn(message, failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable failure) throws E {
        throw (E) failure;
    }
}
