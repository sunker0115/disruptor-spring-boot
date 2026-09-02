package com.sstlfsj.disruptor.core;

import lombok.Builder;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** 监督一组已登记 worker 的启动、失败和终止。 */
public final class WorkerSupervisor {

    private static final long JOIN_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final String name;
    private final int expectedWorkers;
    private final Duration shutdownTimeout;
    private final long shutdownTimeoutNanos;
    private final StopAction stopAction;
    private final Runnable beforeTerminationCommit;
    private final Object stateLock = new Object();
    private final Set<Thread> workers = Collections.newSetFromMap(new IdentityHashMap<>());
    private final CompletableFuture<WorkerSnapshot> termination = new CompletableFuture<>();
    private final CompletionStage<WorkerSnapshot> terminationView = termination.minimalCompletionStage();
    private PipelineLifecycle lifecycle = PipelineLifecycle.NEW;
    private Throwable failure;
    private ShutdownMode shutdownMode;
    private int aliveWorkers;
    private long shutdownDeadlineNanos;
    private Thread controlThread;
    private boolean gracefulTermination;

    private WorkerSupervisor(
            String name,
            int expectedWorkers,
            Duration shutdownTimeout,
            StopAction stopAction) {
        this(name, expectedWorkers, shutdownTimeout, stopAction, () -> { });
    }

    private WorkerSupervisor(
            String name,
            int expectedWorkers,
            Duration shutdownTimeout,
            StopAction stopAction,
            Runnable beforeTerminationCommit) {
        this.name = requireName(name);
        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers 必须大于 0，实际值=" + expectedWorkers);
        }
        this.expectedWorkers = expectedWorkers;
        this.shutdownTimeout = requirePositiveTimeout(shutdownTimeout);
        this.shutdownTimeoutNanos = shutdownTimeout.toNanos();
        this.stopAction = Objects.requireNonNull(stopAction, "stopAction 不能为空");
        this.beforeTerminationCommit = Objects.requireNonNull(
                beforeTerminationCommit, "beforeTerminationCommit 不能为空");
    }

    @Builder(builderMethodName = "builder")
    private static WorkerSupervisor buildSupervisor(
            String name,
            int expectedWorkers,
            Duration shutdownTimeout,
            StopAction stopAction) {
        return new WorkerSupervisor(name, expectedWorkers, shutdownTimeout, stopAction);
    }

    static WorkerSupervisor buildForTesting(
            WorkerSupervisorBuilder builder,
            Runnable beforeTerminationCommit) {
        Objects.requireNonNull(builder, "builder 不能为空");
        return new WorkerSupervisor(
                builder.name,
                builder.expectedWorkers,
                builder.shutdownTimeout,
                builder.stopAction,
                beforeTerminationCommit);
    }

    public Runnable supervise(Runnable worker) {
        Objects.requireNonNull(worker, "worker 不能为空");
        return () -> {
            Thread current = Thread.currentThread();
            synchronized (stateLock) {
                if (!workers.contains(current)) {
                    throw new IllegalStateException("worker 线程必须先登记：" + current.getName());
                }
                if (shutdownMode == ShutdownMode.IMMEDIATE
                        || lifecycle == PipelineLifecycle.TERMINATED) {
                    return;
                }
                aliveWorkers++;
            }
            Throwable thrown = null;
            try {
                worker.run();
            } catch (Throwable workerFailure) {
                thrown = workerFailure;
            } finally {
                ShutdownSignal signal;
                synchronized (stateLock) {
                    aliveWorkers--;
                    signal = recordWorkerExitLocked(current, thrown);
                }
                applyShutdownSignal(signal);
            }
            if (thrown != null) {
                WorkerSupervisor.<RuntimeException>sneakyThrow(thrown);
            }
        };
    }

    public void register(Thread thread) {
        Objects.requireNonNull(thread, "thread 不能为空");
        synchronized (stateLock) {
            PipelineLifecycle current = lifecycle;
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
        synchronized (stateLock) {
            if (lifecycle != PipelineLifecycle.NEW) {
                throw new IllegalStateException("只有 NEW 状态可以进入 STARTING，当前状态=" + lifecycle);
            }
            lifecycle = PipelineLifecycle.STARTING;
        }
    }

    public void markRunning() {
        synchronized (stateLock) {
            if (lifecycle != PipelineLifecycle.STARTING) {
                throw new IllegalStateException(
                        "只有 STARTING 状态可以进入 RUNNING，当前状态=" + lifecycle);
            }
            if (workers.size() != expectedWorkers) {
                throw new IllegalStateException(
                        "进入 RUNNING 前必须登记全部 worker，expected=" + expectedWorkers
                                + "，registered=" + workers.size());
            }
            if (failure != null || shutdownMode != null) {
                throw new IllegalStateException("已失败或已请求停机的 supervisor 不能进入 RUNNING");
            }
            lifecycle = PipelineLifecycle.RUNNING;
        }
    }

    public void fail(Throwable workerFailure) {
        Objects.requireNonNull(workerFailure, "failure 不能为空");
        ShutdownSignal signal;
        synchronized (stateLock) {
            if (lifecycle == PipelineLifecycle.TERMINATED) {
                return;
            }
            if (failure == null) {
                failure = workerFailure;
            }
            signal = prepareShutdownLocked(ShutdownMode.IMMEDIATE);
        }
        applyShutdownSignal(signal);
    }

    public void requestShutdown(ShutdownMode requestedMode) {
        Objects.requireNonNull(requestedMode, "mode 不能为空");
        ShutdownSignal signal;
        synchronized (stateLock) {
            if (lifecycle == PipelineLifecycle.TERMINATED) {
                return;
            }
            signal = prepareShutdownLocked(requestedMode);
        }
        applyShutdownSignal(signal);
    }

    private ShutdownSignal prepareShutdownLocked(ShutdownMode requestedMode) {
        ShutdownMode currentMode = shutdownMode;
        boolean modeChanged = currentMode == null
                || currentMode == ShutdownMode.GRACEFUL && requestedMode == ShutdownMode.IMMEDIATE;
        if (!modeChanged) {
            return ShutdownSignal.NONE;
        }
        shutdownMode = requestedMode;
        moveToShutdownState(requestedMode);
        boolean startControlThread = false;
        if (controlThread == null) {
            shutdownDeadlineNanos = deadlineAfter(shutdownTimeoutNanos);
            controlThread = Thread.ofVirtual()
                    .name("worker-supervisor-" + name)
                    .unstarted(this::controlShutdown);
            startControlThread = true;
        }
        return new ShutdownSignal(
                controlThread,
                startControlThread,
                requestedMode == ShutdownMode.IMMEDIATE);
    }

    private void applyShutdownSignal(ShutdownSignal signal) {
        if (signal.interruptWorkers()) {
            interruptAliveWorkers();
        }
        if (signal.startControlThread()) {
            signal.controlThread().start();
        }
    }

    public CompletionStage<WorkerSnapshot> termination() {
        return terminationView;
    }

    public WorkerSnapshot snapshot() {
        synchronized (stateLock) {
            return snapshotLocked();
        }
    }

    private void controlShutdown() {
        long deadlineNanos;
        synchronized (stateLock) {
            deadlineNanos = shutdownDeadlineNanos;
        }
        ShutdownMode appliedMode = null;
        while (true) {
            ShutdownMode requestedMode = requestedShutdownMode();
            if (requestedMode != appliedMode) {
                if (requestedMode == ShutdownMode.IMMEDIATE) {
                    interruptAliveWorkers();
                }
                invokeStopAction(requestedMode, deadlineNanos);
                appliedMode = requestedMode;
                continue;
            }

            WaitResult waitResult = awaitWorkers(deadlineNanos, appliedMode);
            if (waitResult == WaitResult.UPGRADED) {
                continue;
            }
            if (waitResult == WaitResult.ALL_STOPPED) {
                if (finishTermination(appliedMode)) {
                    return;
                }
                continue;
            }

            applyShutdownSignal(recordTerminationFailure(waitResult, deadlineNanos));
            if (appliedMode != ShutdownMode.IMMEDIATE) {
                continue;
            }
            awaitWorkersAfterDeadline();
            if (finishTermination(appliedMode)) {
                return;
            }
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
                if (requestedShutdownMode() != appliedMode) {
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
        return requestedShutdownMode() == appliedMode
                ? WaitResult.ALL_STOPPED
                : WaitResult.UPGRADED;
    }

    private void awaitWorkersAfterDeadline() {
        boolean interrupted = false;
        for (Thread worker : workerSnapshot()) {
            if (worker == Thread.currentThread()) {
                continue;
            }
            while (worker.isAlive()) {
                try {
                    worker.join();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private ShutdownSignal recordTerminationFailure(WaitResult waitResult, long deadlineNanos) {
        Throwable terminationFailure = waitResult == WaitResult.TIMED_OUT
                ? new WorkerTerminationTimeoutException(
                        name, shutdownTimeout, aliveThreadNames(), deadlineNanos)
                : new IllegalStateException("worker 监督控制线程在等待终止时被中断：" + name);
        synchronized (stateLock) {
            if (lifecycle == PipelineLifecycle.TERMINATED) {
                return ShutdownSignal.NONE;
            }
            if (failure == null) {
                failure = terminationFailure;
            }
            return prepareShutdownLocked(ShutdownMode.IMMEDIATE);
        }
    }

    private boolean finishTermination(ShutdownMode appliedMode) {
        beforeTerminationCommit.run();
        WorkerSnapshot result;
        synchronized (stateLock) {
            if (shutdownMode != appliedMode || aliveWorkers != 0 || hasAliveWorkerLocked()) {
                return false;
            }
            int registeredWorkers = workers.size();
            gracefulTermination = shutdownMode == ShutdownMode.GRACEFUL
                    && failure == null
                    && registeredWorkers == expectedWorkers
                    && aliveWorkers == 0;
            lifecycle = PipelineLifecycle.TERMINATED;
            result = snapshotLocked();
        }
        termination.complete(result);
        return true;
    }

    private boolean hasAliveWorkerLocked() {
        return workers.stream().anyMatch(Thread::isAlive);
    }

    private void moveToShutdownState(ShutdownMode requestedMode) {
        PipelineLifecycle current = lifecycle;
        if (requestedMode == ShutdownMode.GRACEFUL && current == PipelineLifecycle.RUNNING) {
            lifecycle = PipelineLifecycle.QUIESCING;
        } else {
            lifecycle = PipelineLifecycle.STOPPING;
        }
    }

    private void interruptAliveWorkers() {
        for (Thread worker : workerSnapshot()) {
            if (worker != Thread.currentThread() && worker.isAlive()) {
                worker.interrupt();
            }
        }
    }

    private ShutdownSignal recordWorkerExitLocked(Thread worker, Throwable workerFailure) {
        if (shutdownMode != null
                || (lifecycle != PipelineLifecycle.STARTING
                && lifecycle != PipelineLifecycle.RUNNING)) {
            return ShutdownSignal.NONE;
        }
        if (failure == null) {
            failure = workerFailure != null
                    ? workerFailure
                    : new UnexpectedWorkerExitException(name, worker.getName(), lifecycle);
        }
        return prepareShutdownLocked(ShutdownMode.IMMEDIATE);
    }

    private ShutdownMode requestedShutdownMode() {
        synchronized (stateLock) {
            return shutdownMode;
        }
    }

    private WorkerSnapshot snapshotLocked() {
        return WorkerSnapshot.builder()
                .name(name)
                .lifecycle(lifecycle)
                .expectedWorkers(expectedWorkers)
                .registeredWorkers(workers.size())
                .aliveWorkers(aliveWorkers)
                .failure(failure)
                .shutdownMode(shutdownMode)
                .gracefulTermination(gracefulTermination)
                .build();
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

    public static final class UnexpectedWorkerExitException extends IllegalStateException {

        private UnexpectedWorkerExitException(
                String supervisorName,
                String workerName,
                PipelineLifecycle lifecycle) {
            super("worker 在 " + lifecycle + " 状态下意外退出：supervisor=" + supervisorName
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

    private record ShutdownSignal(
            Thread controlThread,
            boolean startControlThread,
            boolean interruptWorkers) {

        private static final ShutdownSignal NONE = new ShutdownSignal(null, false, false);
    }
}
