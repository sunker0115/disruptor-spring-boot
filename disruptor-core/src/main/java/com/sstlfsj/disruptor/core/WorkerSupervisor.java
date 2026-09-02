package com.sstlfsj.disruptor.core;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** 监督一组已登记 worker 的启动、失败和终止。 */
public final class WorkerSupervisor {

    private static final long JOIN_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final String name;
    private final int expectedWorkers;
    private final Duration shutdownTimeout;
    private final long shutdownTimeoutNanos;
    private final StopAction stopAction;
    private final Object stateLock = new Object();
    private final Set<Thread> workers = Collections.newSetFromMap(new IdentityHashMap<>());
    private final AtomicReference<PipelineLifecycle> lifecycle =
            new AtomicReference<>(PipelineLifecycle.NEW);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicReference<ShutdownMode> shutdownMode = new AtomicReference<>();
    private final AtomicInteger aliveWorkers = new AtomicInteger();
    private final CompletableFuture<WorkerSnapshot> termination = new CompletableFuture<>();
    private final CompletionStage<WorkerSnapshot> terminationView = termination.minimalCompletionStage();
    private volatile Thread controlThread;
    private volatile boolean gracefulTermination;

    private WorkerSupervisor(
            String name,
            int expectedWorkers,
            Duration shutdownTimeout,
            StopAction stopAction) {
        this.name = requireName(name);
        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers 必须大于 0，实际值=" + expectedWorkers);
        }
        this.expectedWorkers = expectedWorkers;
        this.shutdownTimeout = requirePositiveTimeout(shutdownTimeout);
        this.shutdownTimeoutNanos = shutdownTimeout.toNanos();
        this.stopAction = Objects.requireNonNull(stopAction, "stopAction 不能为空");
    }

    public static Builder builder() {
        return new Builder();
    }

    public Runnable supervise(Runnable worker) {
        Objects.requireNonNull(worker, "worker 不能为空");
        return () -> {
            Thread current = Thread.currentThread();
            synchronized (stateLock) {
                if (!workers.contains(current)) {
                    throw new IllegalStateException("worker 线程必须先登记：" + current.getName());
                }
            }
            aliveWorkers.incrementAndGet();
            Throwable thrown = null;
            try {
                worker.run();
            } catch (Throwable workerFailure) {
                thrown = workerFailure;
                failWorkerUnlessStopping(workerFailure);
                WorkerSupervisor.<RuntimeException>sneakyThrow(workerFailure);
            } finally {
                aliveWorkers.decrementAndGet();
                if (thrown == null) {
                    failUnexpectedWorkerExit(current);
                }
            }
        };
    }

    public void register(Thread thread) {
        Objects.requireNonNull(thread, "thread 不能为空");
        synchronized (stateLock) {
            PipelineLifecycle current = lifecycle.get();
            if (current != PipelineLifecycle.NEW && current != PipelineLifecycle.STARTING) {
                throw new IllegalStateException("不能在 " + current + " 状态登记 worker");
            }
            if (workers.contains(thread)) {
                throw new IllegalStateException("不能重复登记 worker：" + thread.getName());
            }
            if (workers.size() >= expectedWorkers) {
                throw new IllegalStateException("登记 worker 数不能超过 expectedWorkers=" + expectedWorkers);
            }
            workers.add(thread);
        }
    }

    public void markStarting() {
        if (!lifecycle.compareAndSet(PipelineLifecycle.NEW, PipelineLifecycle.STARTING)) {
            throw new IllegalStateException("只有 NEW 状态可以进入 STARTING，当前状态=" + lifecycle.get());
        }
    }

    public void markRunning() {
        synchronized (stateLock) {
            if (lifecycle.get() != PipelineLifecycle.STARTING) {
                throw new IllegalStateException(
                        "只有 STARTING 状态可以进入 RUNNING，当前状态=" + lifecycle.get());
            }
            if (workers.size() != expectedWorkers) {
                throw new IllegalStateException(
                        "进入 RUNNING 前必须登记全部 worker，expected=" + expectedWorkers
                                + "，registered=" + workers.size());
            }
            if (failure.get() != null || shutdownMode.get() != null) {
                throw new IllegalStateException("已失败或已请求停机的 supervisor 不能进入 RUNNING");
            }
            lifecycle.set(PipelineLifecycle.RUNNING);
        }
    }

    public void fail(Throwable workerFailure) {
        Objects.requireNonNull(workerFailure, "failure 不能为空");
        Thread threadToStart;
        synchronized (stateLock) {
            if (lifecycle.get() == PipelineLifecycle.TERMINATED) {
                return;
            }
            failure.compareAndSet(null, workerFailure);
            threadToStart = prepareShutdownLocked(ShutdownMode.IMMEDIATE);
        }
        startOrWakeControlThread(threadToStart);
    }

    public void requestShutdown(ShutdownMode requestedMode) {
        Objects.requireNonNull(requestedMode, "mode 不能为空");
        Thread threadToStart;
        synchronized (stateLock) {
            if (lifecycle.get() == PipelineLifecycle.TERMINATED) {
                return;
            }
            threadToStart = prepareShutdownLocked(requestedMode);
        }
        startOrWakeControlThread(threadToStart);
    }

    private Thread prepareShutdownLocked(ShutdownMode requestedMode) {
        ShutdownMode currentMode = shutdownMode.get();
        if (currentMode == null) {
            shutdownMode.set(requestedMode);
            moveToShutdownState(requestedMode);
        } else if (currentMode == ShutdownMode.GRACEFUL
                && requestedMode == ShutdownMode.IMMEDIATE) {
            shutdownMode.set(ShutdownMode.IMMEDIATE);
            lifecycle.set(PipelineLifecycle.STOPPING);
        }
        if (controlThread == null) {
            controlThread = Thread.ofVirtual()
                    .name("worker-supervisor-" + name)
                    .unstarted(this::controlShutdown);
            return controlThread;
        }
        return null;
    }

    private void startOrWakeControlThread(Thread threadToStart) {
        if (threadToStart != null) {
            threadToStart.start();
        } else {
            java.util.concurrent.locks.LockSupport.unpark(controlThread);
        }
    }

    public CompletionStage<WorkerSnapshot> termination() {
        return terminationView;
    }

    public WorkerSnapshot snapshot() {
        synchronized (stateLock) {
            return new WorkerSnapshot(
                    name,
                    lifecycle.get(),
                    expectedWorkers,
                    workers.size(),
                    aliveWorkers.get(),
                    failure.get(),
                    shutdownMode.get(),
                    gracefulTermination);
        }
    }

    private void controlShutdown() {
        long deadlineNanos = deadlineAfter(shutdownTimeoutNanos);
        ShutdownMode appliedMode = null;
        boolean allStopped = false;
        try {
            while (true) {
                ShutdownMode requestedMode = shutdownMode.get();
                if (requestedMode != appliedMode) {
                    if (requestedMode == ShutdownMode.IMMEDIATE) {
                        interruptAliveWorkers();
                    }
                    invokeStopAction(requestedMode, deadlineNanos);
                    appliedMode = requestedMode;
                    continue;
                }

                WaitResult waitResult = awaitWorkers(deadlineNanos, appliedMode);
                if (waitResult == WaitResult.ALL_STOPPED) {
                    allStopped = true;
                    break;
                }
                if (waitResult == WaitResult.UPGRADED) {
                    continue;
                }
                recordTerminationFailure(waitResult, deadlineNanos);
                interruptAliveWorkers();
                break;
            }
        } finally {
            finishTermination(allStopped || allWorkersStopped());
        }
    }

    private void invokeStopAction(ShutdownMode mode, long deadlineNanos) {
        try {
            stopAction.stop(mode, deadlineNanos);
        } catch (Throwable stopFailure) {
            fail(stopFailure);
        }
    }

    private WaitResult awaitWorkers(long deadlineNanos, ShutdownMode appliedMode) {
        for (Thread worker : workerSnapshot()) {
            if (worker == Thread.currentThread()) {
                continue;
            }
            while (worker.isAlive()) {
                if (shutdownMode.get() != appliedMode) {
                    return WaitResult.UPGRADED;
                }
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0L) {
                    return WaitResult.TIMED_OUT;
                }
                long joinNanos = Math.min(remaining, JOIN_SLICE_NANOS);
                try {
                    worker.join(joinNanos / 1_000_000L, (int) (joinNanos % 1_000_000L));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return WaitResult.INTERRUPTED;
                }
            }
        }
        return WaitResult.ALL_STOPPED;
    }

    private void recordTerminationFailure(WaitResult waitResult, long deadlineNanos) {
        if (waitResult == WaitResult.TIMED_OUT) {
            fail(new WorkerTerminationTimeoutException(
                    name, shutdownTimeout, aliveThreadNames(), deadlineNanos));
        } else {
            fail(new IllegalStateException("worker 监督控制线程在等待终止时被中断：" + name));
        }
    }

    private void finishTermination(boolean allStopped) {
        WorkerSnapshot result;
        synchronized (stateLock) {
            int registeredWorkers = workers.size();
            gracefulTermination = shutdownMode.get() == ShutdownMode.GRACEFUL
                    && failure.get() == null
                    && registeredWorkers == expectedWorkers
                    && allStopped
                    && aliveWorkers.get() == 0;
            lifecycle.set(PipelineLifecycle.TERMINATED);
            result = new WorkerSnapshot(
                    name,
                    PipelineLifecycle.TERMINATED,
                    expectedWorkers,
                    registeredWorkers,
                    aliveWorkers.get(),
                    failure.get(),
                    shutdownMode.get(),
                    gracefulTermination);
        }
        termination.complete(result);
    }

    private void moveToShutdownState(ShutdownMode requestedMode) {
        PipelineLifecycle current = lifecycle.get();
        if (requestedMode == ShutdownMode.GRACEFUL && current == PipelineLifecycle.RUNNING) {
            lifecycle.set(PipelineLifecycle.QUIESCING);
        } else {
            lifecycle.set(PipelineLifecycle.STOPPING);
        }
    }

    private void interruptAliveWorkers() {
        for (Thread worker : workerSnapshot()) {
            if (worker != Thread.currentThread() && worker.isAlive()) {
                worker.interrupt();
            }
        }
    }

    private void failWorkerUnlessStopping(Throwable workerFailure) {
        Thread threadToStart;
        synchronized (stateLock) {
            if (shutdownMode.get() != null || lifecycle.get() == PipelineLifecycle.TERMINATED) {
                return;
            }
            failure.compareAndSet(null, workerFailure);
            threadToStart = prepareShutdownLocked(ShutdownMode.IMMEDIATE);
        }
        startOrWakeControlThread(threadToStart);
    }

    private void failUnexpectedWorkerExit(Thread worker) {
        Thread threadToStart;
        synchronized (stateLock) {
            PipelineLifecycle current = lifecycle.get();
            if (shutdownMode.get() != null
                    || (current != PipelineLifecycle.STARTING
                    && current != PipelineLifecycle.RUNNING)) {
                return;
            }
            failure.compareAndSet(null, new UnexpectedWorkerExitException(name, worker.getName()));
            threadToStart = prepareShutdownLocked(ShutdownMode.IMMEDIATE);
        }
        startOrWakeControlThread(threadToStart);
    }

    private boolean allWorkersStopped() {
        for (Thread worker : workerSnapshot()) {
            if (worker != Thread.currentThread() && worker.isAlive()) {
                return false;
            }
        }
        return true;
    }

    private List<String> aliveThreadNames() {
        return workerSnapshot().stream().filter(Thread::isAlive).map(Thread::getName).toList();
    }

    private List<Thread> workerSnapshot() {
        synchronized (stateLock) {
            return List.copyOf(workers);
        }
    }

    private static String requireName(String value) {
        Objects.requireNonNull(value, "name 不能为空");
        if (value.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        return value;
    }

    private static Duration requirePositiveTimeout(Duration value) {
        Objects.requireNonNull(value, "shutdownTimeout 不能为空");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout 必须大于 0，实际值=" + value);
        }
        value.toNanos();
        return value;
    }

    private static long deadlineAfter(long timeoutNanos) {
        long now = System.nanoTime();
        try {
            return Math.addExact(now, timeoutNanos);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }

    @FunctionalInterface
    public interface StopAction {
        void stop(ShutdownMode mode, long deadlineNanos) throws Throwable;
    }

    public static final class Builder {

        private String name;
        private int expectedWorkers;
        private Duration shutdownTimeout;
        private StopAction stopAction;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder expectedWorkers(int expectedWorkers) {
            this.expectedWorkers = expectedWorkers;
            return this;
        }

        public Builder shutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public Builder stopAction(StopAction stopAction) {
            this.stopAction = stopAction;
            return this;
        }

        public WorkerSupervisor build() {
            return new WorkerSupervisor(name, expectedWorkers, shutdownTimeout, stopAction);
        }
    }

    public static final class UnexpectedWorkerExitException extends IllegalStateException {

        private UnexpectedWorkerExitException(String supervisorName, String workerName) {
            super("worker 在 RUNNING 状态下意外退出：supervisor=" + supervisorName
                    + "，worker=" + workerName);
        }
    }

    public static final class WorkerTerminationTimeoutException extends IllegalStateException {

        private WorkerTerminationTimeoutException(
                String supervisorName,
                Duration timeout,
                List<String> aliveWorkers,
                long deadlineNanos) {
            super("等待 worker 终止超时：supervisor=" + supervisorName
                    + "，timeout=" + timeout
                    + "，deadlineNanos=" + deadlineNanos
                    + "，aliveWorkers=" + aliveWorkers);
        }
    }

    private enum WaitResult {
        ALL_STOPPED,
        UPGRADED,
        TIMED_OUT,
        INTERRUPTED
    }
}
