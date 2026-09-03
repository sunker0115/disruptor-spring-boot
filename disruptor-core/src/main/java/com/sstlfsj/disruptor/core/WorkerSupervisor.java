package com.sstlfsj.disruptor.core;

import lombok.Builder;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * 统一监督动态 worker 集的启动、关闭、故障和真实线程终止。
 *
 * <p>{@link ShutdownBackend} 是队列实现接入 supervisor 的内部 SPI，不是任意用户回调。
 * supervisor 只会从自己的命名虚拟控制线程串行调用它。</p>
 */
public final class WorkerSupervisor {

    private static final long CONTROL_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long DRAIN_PROBE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private final String name;
    private final Duration shutdownTimeout;
    private final ShutdownBackend shutdownBackend;
    private final Object stateLock = new Object();
    private final Map<Thread, WorkerState> workers = new IdentityHashMap<>();
    private final Set<Thread> joinedWorkers = Collections.newSetFromMap(new IdentityHashMap<>());
    private final CompletableFuture<Void> workersStarted = new CompletableFuture<>();
    private final CompletionStage<Void> workersStartedView = workersStarted.minimalCompletionStage();
    private final CompletableFuture<WorkerSnapshot> termination = new CompletableFuture<>();
    private final CompletionStage<WorkerSnapshot> terminationView = termination.minimalCompletionStage();

    private PipelineLifecycle lifecycle = PipelineLifecycle.NEW;
    private boolean startupInitiated;
    private boolean registrationSealed;
    private StartupOutcome startupOutcome = StartupOutcome.PENDING;
    private Throwable startupOutcomeFailure;
    private boolean startupOutcomePublicationScheduled;
    private int expectedWorkers;
    private int startedWorkers;
    private int aliveWorkers;
    private Throwable failure;
    private ShutdownMode shutdownMode;
    private ShutdownDeadline shutdownDeadline;
    private boolean reachedRunning;
    private boolean drainCommitted;
    private boolean gracefulStopApplied;
    private boolean beginQuiesceAttempted;
    private boolean gracefulStopAttempted;
    private boolean gracefulStopFinished;
    private boolean immediateStopAttempted;
    private boolean immediateStopFinished;
    private boolean timeoutHandled;
    private Thread controlThread;

    private WorkerSupervisor(String name, Duration shutdownTimeout, ShutdownBackend shutdownBackend) {
        this.name = requireName(name);
        this.shutdownTimeout = requirePositiveTimeout(shutdownTimeout);
        this.shutdownBackend = Objects.requireNonNull(shutdownBackend, "shutdownBackend 不能为空");
    }

    /**
     * 构造 supervisor 的内部接入点。
     *
     * @param shutdownBackend 满足 {@link ShutdownBackend} 非阻塞阶段契约的队列后端 SPI
     */
    @Builder(builderMethodName = "builder")
    private static WorkerSupervisor buildSupervisor(
            String name,
            Duration shutdownTimeout,
            ShutdownBackend shutdownBackend) {
        return new WorkerSupervisor(name, shutdownTimeout, shutdownBackend);
    }

    public Runnable supervise(Runnable worker) {
        Objects.requireNonNull(worker, "worker 不能为空");
        return () -> runSupervised(worker);
    }

    /**
     * 登记尚未启动的 worker。允许预构造 worker 在 {@link PipelineLifecycle#NEW} 登记；
     * 启动协议已进入 {@link PipelineLifecycle#STARTING} 后也可动态登记。若并发关闭先把生命周期
     * 推进到 {@link PipelineLifecycle#STOPPING}，尚未结束的启动协议仍可继续登记，直至封口。
     */
    public void register(Thread thread) {
        Objects.requireNonNull(thread, "thread 不能为空");
        synchronized (stateLock) {
            boolean startupFinishingWhileStopping = lifecycle == PipelineLifecycle.STOPPING
                    && startupInitiated
                    && !registrationSealed;
            if (lifecycle != PipelineLifecycle.NEW
                    && lifecycle != PipelineLifecycle.STARTING
                    && !startupFinishingWhileStopping) {
                throw new IllegalStateException("不能在 " + lifecycle + " 状态登记 worker");
            }
            if (registrationSealed) {
                throw new IllegalStateException("worker 登记已经封口");
            }
            if (thread.getState() != Thread.State.NEW) {
                throw new IllegalStateException("只能登记尚未启动的 worker：" + thread.getName());
            }
            if (workers.putIfAbsent(thread, WorkerState.REGISTERED) != null) {
                throw new IllegalStateException("不能重复登记 worker：" + thread.getName());
            }
        }
    }

    public void markStarting() {
        synchronized (stateLock) {
            if (lifecycle != PipelineLifecycle.NEW) {
                throw new IllegalStateException("只有 NEW 状态可以进入 STARTING，当前状态=" + lifecycle);
            }
            startupInitiated = true;
            lifecycle = PipelineLifecycle.STARTING;
            stateLock.notifyAll();
        }
    }

    /**
     * 封口当前启动协议的 worker 集。
     *
     * <p>调用方只能在 {@link #markStarting()} 之后调用，并且必须保证全部线程创建以及所有
     * {@link Thread#start()} 调用都已返回，之后不会再尝试启动或登记 worker。并发关闭可能已把
     * 生命周期推进到 {@link PipelineLifecycle#STOPPING}，启动方仍必须在自己的 finally 路径封口。</p>
     */
    public void sealWorkers() {
        StartupNotification startupNotification;
        synchronized (stateLock) {
            if (!startupInitiated
                    || (lifecycle != PipelineLifecycle.STARTING
                    && lifecycle != PipelineLifecycle.STOPPING)) {
                throw new IllegalStateException("不能在 " + lifecycle + " 状态封口 worker 登记");
            }
            if (registrationSealed) {
                throw new IllegalStateException("worker 登记已经封口");
            }
            sealRegistrationLocked();
            startupNotification = commitStartupSuccessIfReadyLocked();
            stateLock.notifyAll();
        }
        publishStartupOutcome(startupNotification);
    }

    /**
     * 返回只读启动结果。结果先在状态锁内唯一提交，再由专用命名虚拟线程发布，用户依赖回调不会
     * 同步占用 worker、关闭请求调用线程或 supervisor 控制线程。
     */
    public CompletionStage<Void> workersStarted() {
        return workersStartedView;
    }

    public void markRunning() {
        synchronized (stateLock) {
            if (lifecycle != PipelineLifecycle.STARTING) {
                throw new IllegalStateException("只有 STARTING 状态可以进入 RUNNING，当前状态=" + lifecycle);
            }
            if (!registrationSealed || expectedWorkers == 0) {
                throw new IllegalStateException("进入 RUNNING 前必须封口至少一个 worker");
            }
            if (startedWorkers != expectedWorkers || aliveWorkers != expectedWorkers) {
                throw new IllegalStateException("进入 RUNNING 前全部 worker 必须已经入场且存活");
            }
            if (failure != null || shutdownMode != null) {
                throw new IllegalStateException("已失败或已请求关闭的 supervisor 不能进入 RUNNING");
            }
            reachedRunning = true;
            lifecycle = PipelineLifecycle.RUNNING;
            stateLock.notifyAll();
        }
    }

    public void fail(Throwable cause) {
        Objects.requireNonNull(cause, "failure 不能为空");
        ShutdownSignals signals;
        synchronized (stateLock) {
            if (lifecycle == PipelineLifecycle.TERMINATED) {
                return;
            }
            signals = requestImmediateLocked(cause, defaultDeadlineLocked());
        }
        apply(signals);
    }

    public void requestShutdown(ShutdownMode mode) {
        requestShutdown(mode, ShutdownDeadline.after(shutdownTimeout));
    }

    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        Objects.requireNonNull(mode, "mode 不能为空");
        Objects.requireNonNull(deadline, "deadline 不能为空");
        ShutdownSignals signals;
        synchronized (stateLock) {
            if (lifecycle == PipelineLifecycle.TERMINATED) {
                return;
            }
            freezeDeadlineLocked(deadline);
            boolean abortStartup = lifecycle == PipelineLifecycle.NEW
                    || lifecycle == PipelineLifecycle.STARTING;
            if (lifecycle == PipelineLifecycle.NEW) {
                sealRegistrationLocked();
            }
            boolean firstRequest = shutdownMode == null;
            if (firstRequest) {
                shutdownMode = mode;
            } else if (mode == ShutdownMode.IMMEDIATE) {
                shutdownMode = ShutdownMode.IMMEDIATE;
            }
            if (shutdownMode == ShutdownMode.IMMEDIATE) {
                lifecycle = PipelineLifecycle.STOPPING;
            } else if (firstRequest) {
                lifecycle = lifecycle == PipelineLifecycle.RUNNING
                        ? PipelineLifecycle.QUIESCING
                        : PipelineLifecycle.STOPPING;
            }
            Thread toStart = ensureControlThreadLocked();
            stateLock.notifyAll();
            signals = new ShutdownSignals(
                    toStart,
                    abortStartup ? commitStartupFailureLocked() : null);
        }
        apply(signals);
    }

    /**
     * 返回只读终止信号。该信号只会在后端停止完成、全部已启动 worker 完成 join 且终态提交后发布；
     * 因此同步执行的依赖回调不会阻塞任何内部生命周期动作。
     */
    public CompletionStage<WorkerSnapshot> termination() {
        return terminationView;
    }

    public WorkerSnapshot snapshot() {
        synchronized (stateLock) {
            return snapshotLocked();
        }
    }

    private void runSupervised(Runnable worker) {
        WorkerAdmission admission = admitWorker(Thread.currentThread());
        apply(admission.signals());
        if (admission.failure() != null) {
            sneakyThrow(admission.failure());
        }

        Throwable thrown = null;
        try {
            worker.run();
        } catch (Throwable workerFailure) {
            thrown = workerFailure;
        } finally {
            apply(exitWorker(Thread.currentThread(), thrown));
        }
        if (thrown != null) {
            sneakyThrow(thrown);
        }
    }

    private WorkerAdmission admitWorker(Thread current) {
        synchronized (stateLock) {
            WorkerState workerState = workers.get(current);
            if (workerState == null) {
                IllegalStateException admissionFailure = new IllegalStateException(
                        "worker 线程必须先登记：" + current.getName());
                return new WorkerAdmission(admissionFailure,
                        requestImmediateLocked(admissionFailure, defaultDeadlineLocked()));
            }
            if (workerState != WorkerState.REGISTERED) {
                IllegalStateException admissionFailure = new IllegalStateException(
                        "worker 线程不能重复入场：" + current.getName());
                return new WorkerAdmission(admissionFailure,
                        requestImmediateLocked(admissionFailure, defaultDeadlineLocked()));
            }
            if (lifecycle != PipelineLifecycle.STARTING && lifecycle != PipelineLifecycle.RUNNING) {
                IllegalStateException admissionFailure = new IllegalStateException(
                        "worker 不能在 " + lifecycle + " 状态入场：" + current.getName());
                if (lifecycle == PipelineLifecycle.TERMINATED) {
                    return new WorkerAdmission(admissionFailure, ShutdownSignals.NONE);
                }
                workers.put(current, WorkerState.EXITED);
                startedWorkers++;
                ShutdownSignals signals = lifecycle == PipelineLifecycle.NEW
                        ? requestImmediateLocked(admissionFailure, defaultDeadlineLocked())
                        : ShutdownSignals.NONE;
                stateLock.notifyAll();
                return new WorkerAdmission(admissionFailure, signals);
            }

            workers.put(current, WorkerState.ALIVE);
            startedWorkers++;
            aliveWorkers++;
            StartupNotification startupNotification = commitStartupSuccessIfReadyLocked();
            stateLock.notifyAll();
            return new WorkerAdmission(null,
                    new ShutdownSignals(null, startupNotification));
        }
    }

    private ShutdownSignals exitWorker(Thread worker, Throwable workerFailure) {
        synchronized (stateLock) {
            if (workers.get(worker) != WorkerState.ALIVE) {
                return ShutdownSignals.NONE;
            }
            workers.put(worker, WorkerState.EXITED);
            aliveWorkers--;
            ShutdownSignals signals = ShutdownSignals.NONE;
            if (lifecycle == PipelineLifecycle.STARTING
                    || lifecycle == PipelineLifecycle.RUNNING
                    || lifecycle == PipelineLifecycle.QUIESCING
                    || (lifecycle == PipelineLifecycle.STOPPING && workerFailure != null)) {
                Throwable exitFailure = workerFailure != null
                        ? workerFailure
                        : new UnexpectedWorkerExitException(name, worker.getName(), lifecycle);
                signals = requestImmediateLocked(exitFailure, defaultDeadlineLocked());
            }
            stateLock.notifyAll();
            return signals;
        }
    }

    private ShutdownSignals requestImmediateLocked(Throwable cause, ShutdownDeadline deadline) {
        if (lifecycle == PipelineLifecycle.TERMINATED) {
            return ShutdownSignals.NONE;
        }
        if (failure == null) {
            failure = cause;
        }
        freezeDeadlineLocked(deadline);
        boolean abortStartup = lifecycle == PipelineLifecycle.NEW
                || lifecycle == PipelineLifecycle.STARTING;
        if (lifecycle == PipelineLifecycle.NEW) {
            sealRegistrationLocked();
        }
        shutdownMode = ShutdownMode.IMMEDIATE;
        lifecycle = PipelineLifecycle.STOPPING;
        Thread toStart = ensureControlThreadLocked();
        stateLock.notifyAll();
        return new ShutdownSignals(toStart,
                abortStartup ? commitStartupFailureLocked() : null);
    }

    private void controlShutdown() {
        while (true) {
            ControlStep step;
            WorkerSnapshot terminated = null;
            ShutdownSignals signals = ShutdownSignals.NONE;
            synchronized (stateLock) {
                if (!timeoutHandled && shutdownDeadline.isExpired()) {
                    timeoutHandled = true;
                    Throwable timeout = new WorkerTerminationTimeoutException(
                            name, shutdownDeadline.deadlineNanos(), aliveThreadNamesLocked());
                    signals = requestImmediateLocked(timeout, shutdownDeadline);
                }
                if (canTerminateLocked()) {
                    lifecycle = PipelineLifecycle.TERMINATED;
                    terminated = snapshotLocked();
                    step = ControlStep.TERMINATE;
                } else {
                    step = nextControlStepLocked();
                }
            }
            apply(signals);
            if (terminated != null) {
                termination.complete(terminated);
                return;
            }
            executeControlStep(step);
        }
    }

    private ControlStep nextControlStepLocked() {
        if (shutdownMode == ShutdownMode.IMMEDIATE) {
            lifecycle = PipelineLifecycle.STOPPING;
            if (!immediateStopAttempted) {
                immediateStopAttempted = true;
                return ControlStep.STOP_IMMEDIATE;
            }
            return ControlStep.WAIT;
        }
        if (lifecycle == PipelineLifecycle.QUIESCING) {
            if (!beginQuiesceAttempted) {
                beginQuiesceAttempted = true;
                return ControlStep.BEGIN_QUIESCE;
            }
            return ControlStep.PROBE_DRAIN;
        }
        if (!gracefulStopAttempted) {
            gracefulStopAttempted = true;
            return ControlStep.STOP_GRACEFUL;
        }
        return ControlStep.WAIT;
    }

    private void executeControlStep(ControlStep step) {
        switch (step) {
            case BEGIN_QUIESCE -> invokeBackend(shutdownBackend::beginQuiesce);
            case PROBE_DRAIN -> probeDrain();
            case STOP_GRACEFUL -> stopBackend(ShutdownMode.GRACEFUL);
            case STOP_IMMEDIATE -> stopBackend(ShutdownMode.IMMEDIATE);
            case WAIT -> awaitWorkerOrStateChange();
            case TERMINATE -> throw new IllegalStateException("TERMINATE 不应在控制循环内继续执行");
        }
    }

    private void probeDrain() {
        boolean drained;
        try {
            drained = shutdownBackend.isDrained();
        } catch (Throwable backendFailure) {
            applyBackendFailure(backendFailure);
            return;
        }
        if (drained) {
            synchronized (stateLock) {
                if (shutdownMode == ShutdownMode.GRACEFUL
                        && lifecycle == PipelineLifecycle.QUIESCING
                        && !shutdownDeadline.isExpired()) {
                    drainCommitted = true;
                    lifecycle = PipelineLifecycle.STOPPING;
                    stateLock.notifyAll();
                }
            }
        } else {
            long remainingNanos = shutdownDeadline.remainingNanos();
            if (remainingNanos > 0L) {
                awaitStateChange(Math.min(DRAIN_PROBE_INTERVAL_NANOS, remainingNanos));
            }
        }
    }

    private void stopBackend(ShutdownMode mode) {
        Throwable backendFailure = null;
        try {
            shutdownBackend.stop(mode);
            if (mode == ShutdownMode.GRACEFUL) {
                synchronized (stateLock) {
                    gracefulStopApplied = true;
                }
            }
        } catch (Throwable failure) {
            backendFailure = failure;
        } finally {
            if (backendFailure != null) {
                applyBackendFailure(backendFailure);
            }
            synchronized (stateLock) {
                if (mode == ShutdownMode.GRACEFUL) {
                    gracefulStopFinished = true;
                } else {
                    immediateStopFinished = true;
                }
                stateLock.notifyAll();
            }
            if (mode == ShutdownMode.IMMEDIATE) {
                interruptWorkers();
            }
        }
    }

    private void invokeBackend(BackendAction action) {
        try {
            action.run();
        } catch (Throwable backendFailure) {
            applyBackendFailure(backendFailure);
        }
    }

    private void applyBackendFailure(Throwable backendFailure) {
        ShutdownSignals signals;
        synchronized (stateLock) {
            signals = requestImmediateLocked(backendFailure, shutdownDeadline);
        }
        apply(signals);
    }

    private void awaitWorkerOrStateChange() {
        Thread worker = nextUnjoinedWorker();
        if (worker == null) {
            awaitStateChange(CONTROL_WAIT_NANOS);
            return;
        }
        long waitNanos = timeoutHandled
                ? CONTROL_WAIT_NANOS
                : Math.max(1L, Math.min(CONTROL_WAIT_NANOS, shutdownDeadline.remainingNanos()));
        try {
            long waitMillis = TimeUnit.NANOSECONDS.toMillis(waitNanos);
            int extraNanos = (int) (waitNanos - TimeUnit.MILLISECONDS.toNanos(waitMillis));
            worker.join(waitMillis, extraNanos);
        } catch (InterruptedException interrupted) {
            Thread.interrupted();
            applyBackendFailure(new ControlThreadInterruptedException(name, interrupted));
            return;
        }
        if (!worker.isAlive()) {
            synchronized (stateLock) {
                joinedWorkers.add(worker);
                stateLock.notifyAll();
            }
        }
    }

    private void awaitStateChange(long waitNanos) {
        if (waitNanos <= 0L) {
            return;
        }
        ShutdownSignals signals = ShutdownSignals.NONE;
        synchronized (stateLock) {
            try {
                TimeUnit.NANOSECONDS.timedWait(stateLock, waitNanos);
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                signals = requestImmediateLocked(
                        new ControlThreadInterruptedException(name, interrupted), shutdownDeadline);
            }
        }
        apply(signals);
    }

    private boolean canTerminateLocked() {
        if (lifecycle != PipelineLifecycle.STOPPING || !registrationSealed) {
            return false;
        }
        boolean stopFinished = shutdownMode == ShutdownMode.GRACEFUL
                ? gracefulStopFinished
                : immediateStopFinished;
        return stopFinished && workers.entrySet().stream().allMatch(entry ->
                joinedWorkers.contains(entry.getKey())
                        || (entry.getValue() == WorkerState.REGISTERED
                        && !entry.getKey().isAlive()));
    }

    private Thread nextUnjoinedWorker() {
        synchronized (stateLock) {
            return workers.entrySet().stream()
                    .filter(entry -> entry.getValue() != WorkerState.REGISTERED
                            || entry.getKey().isAlive())
                    .map(Map.Entry::getKey)
                    .filter(worker -> !joinedWorkers.contains(worker))
                    .findFirst()
                    .orElse(null);
        }
    }

    private List<Thread> startedWorkerThreads() {
        synchronized (stateLock) {
            return workers.entrySet().stream()
                    .filter(entry -> entry.getValue() != WorkerState.REGISTERED
                            || entry.getKey().isAlive())
                    .map(Map.Entry::getKey)
                    .toList();
        }
    }

    private List<String> aliveThreadNamesLocked() {
        return workers.keySet().stream().filter(Thread::isAlive).map(Thread::getName).toList();
    }

    private void apply(ShutdownSignals signals) {
        publishStartupOutcome(signals.startupNotification());
        if (signals.controlThreadToStart() != null) {
            signals.controlThreadToStart().start();
        }
    }

    private void interruptWorkers() {
        Thread caller = Thread.currentThread();
        for (Thread worker : startedWorkerThreads()) {
            if (worker != caller && worker.isAlive()) {
                worker.interrupt();
            }
        }
    }

    private Thread ensureControlThreadLocked() {
        if (controlThread != null) {
            return null;
        }
        controlThread = Thread.ofVirtual()
                .name("worker-supervisor-" + name)
                .unstarted(this::controlShutdown);
        return controlThread;
    }

    private ShutdownDeadline defaultDeadlineLocked() {
        return shutdownDeadline != null ? shutdownDeadline : ShutdownDeadline.after(shutdownTimeout);
    }

    private void freezeDeadlineLocked(ShutdownDeadline deadline) {
        if (shutdownDeadline == null) {
            shutdownDeadline = deadline;
        }
    }

    private StartupNotification commitStartupSuccessIfReadyLocked() {
        if (!registrationSealed || startedWorkers != expectedWorkers) {
            return null;
        }
        return commitStartupOutcomeLocked(StartupOutcome.SUCCESS, null);
    }

    private StartupNotification commitStartupFailureLocked() {
        if (startupOutcome != StartupOutcome.PENDING) {
            return null;
        }
        return commitStartupOutcomeLocked(StartupOutcome.FAILURE, startupAbortedFailureLocked());
    }

    private StartupNotification commitStartupOutcomeLocked(
            StartupOutcome outcome,
            Throwable outcomeFailure) {
        if (startupOutcome != StartupOutcome.PENDING) {
            return null;
        }
        if (startupOutcomePublicationScheduled) {
            throw new IllegalStateException("启动结果通知已调度但结果仍为 PENDING");
        }
        if (outcome == StartupOutcome.PENDING) {
            throw new IllegalArgumentException("不能提交 PENDING 启动结果");
        }
        if (outcome == StartupOutcome.FAILURE) {
            Objects.requireNonNull(outcomeFailure, "启动失败原因不能为空");
        }
        startupOutcome = outcome;
        startupOutcomeFailure = outcomeFailure;
        startupOutcomePublicationScheduled = true;
        return new StartupNotification(startupOutcome, startupOutcomeFailure);
    }

    private void publishStartupOutcome(StartupNotification notification) {
        if (notification == null) {
            return;
        }
        Thread.ofVirtual()
                .name("worker-supervisor-" + name + "-startup-notifier")
                .start(() -> {
                    if (notification.outcome() == StartupOutcome.SUCCESS) {
                        workersStarted.complete(null);
                    } else {
                        workersStarted.completeExceptionally(notification.failure());
                    }
                });
    }

    private void sealRegistrationLocked() {
        registrationSealed = true;
        expectedWorkers = workers.size();
    }

    private Throwable startupAbortedFailureLocked() {
        return failure != null ? failure : new StartupAbortedException(name);
    }

    private WorkerSnapshot snapshotLocked() {
        return WorkerSnapshot.builder()
                .name(name)
                .lifecycle(lifecycle)
                .registrationSealed(registrationSealed)
                .expectedWorkers(expectedWorkers)
                .registeredWorkers(workers.size())
                .startedWorkers(startedWorkers)
                .aliveWorkers(aliveWorkers)
                .failure(failure)
                .shutdownMode(shutdownMode)
                .reachedRunning(reachedRunning)
                .drainCommitted(drainCommitted)
                .gracefulStopApplied(gracefulStopApplied)
                .build();
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
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }

    public static final class UnexpectedWorkerExitException extends IllegalStateException {
        private UnexpectedWorkerExitException(String supervisorName, String workerName,
                                              PipelineLifecycle lifecycle) {
            super("worker 在 " + lifecycle + " 状态下意外退出：supervisor=" + supervisorName
                    + "，worker=" + workerName);
        }
    }

    public static final class WorkerTerminationTimeoutException extends IllegalStateException {
        private WorkerTerminationTimeoutException(
                String supervisorName,
                long deadlineNanos,
                List<String> aliveWorkers) {
            super("等待 worker 终止超时：supervisor=" + supervisorName
                    + "，deadlineNanos=" + deadlineNanos
                    + "，aliveWorkers=" + aliveWorkers);
        }
    }

    private static final class StartupAbortedException extends IllegalStateException {
        private StartupAbortedException(String supervisorName) {
            super("worker 启动在完成前被关闭：supervisor=" + supervisorName);
        }
    }

    private static final class ControlThreadInterruptedException extends IllegalStateException {
        private ControlThreadInterruptedException(String supervisorName, InterruptedException cause) {
            super("worker 监督控制线程被中断：supervisor=" + supervisorName, cause);
        }
    }

    private enum WorkerState {
        REGISTERED,
        ALIVE,
        EXITED
    }

    private enum StartupOutcome {
        PENDING,
        SUCCESS,
        FAILURE
    }

    private enum ControlStep {
        BEGIN_QUIESCE,
        PROBE_DRAIN,
        STOP_GRACEFUL,
        STOP_IMMEDIATE,
        WAIT,
        TERMINATE
    }

    @FunctionalInterface
    private interface BackendAction {
        void run() throws Throwable;
    }

    private record WorkerAdmission(Throwable failure, ShutdownSignals signals) { }

    private record StartupNotification(StartupOutcome outcome, Throwable failure) { }

    private record ShutdownSignals(
            Thread controlThreadToStart,
            StartupNotification startupNotification) {
        private static final ShutdownSignals NONE = new ShutdownSignals(null, null);
    }
}
