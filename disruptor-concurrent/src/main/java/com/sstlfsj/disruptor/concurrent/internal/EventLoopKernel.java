package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.BlockingOperationException;
import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.CapacityMode;
import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopModule;
import com.sstlfsj.disruptor.concurrent.EventLoopScheduledFuture;
import com.sstlfsj.disruptor.concurrent.EventLoopSnapshot;
import com.sstlfsj.disruptor.concurrent.NanoClock;
import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSpec;
import com.sstlfsj.disruptor.concurrent.TaskExceptionHandler;
import com.sstlfsj.disruptor.concurrent.TaskOutcome;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import com.sstlfsj.disruptor.core.WorkerSnapshot;
import com.sstlfsj.disruptor.core.WorkerSupervisor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** 有界与无界门面共享的唯一执行、调度和生命周期内核。 */
public final class EventLoopKernel {

    private final EventLoop owner;
    private final String name;
    private final CapacityMode capacityMode;
    private final OptionalLong capacityLimit;
    private final TaskQueue queue;
    private final TaskAdmissionGate gate;
    private final AcceptedTaskRegistry registry = new AcceptedTaskRegistry();
    private final CancellationMailbox cancellationMailbox = new CancellationMailbox();
    private final IndexedScheduledHeap timers = new IndexedScheduledHeap();
    private final ModuleLifecycle modules;
    private final NanoClock clock;
    private final int maxCommandBatchSize;
    private final int maxTimerBatchSize;
    private final TaskExceptionHandler taskExceptionHandler;
    private final WorkerSupervisor supervisor;
    private final Thread worker;
    private final CompletableFuture<Void> startup = new CompletableFuture<>();
    private final CompletionStage<Void> startupView = startup.minimalCompletionStage();
    private final CompletableFuture<EventLoopSnapshot> termination = new CompletableFuture<>();
    private final CompletionStage<EventLoopSnapshot> terminationView =
            termination.minimalCompletionStage();
    private final AtomicBoolean startRequested = new AtomicBoolean();
    private final AtomicBoolean startupOutcomeClaimed = new AtomicBoolean();
    private final AtomicBoolean quiescing = new AtomicBoolean();
    private final AtomicReference<ShutdownMode> stopMode = new AtomicReference<>();
    private final AtomicBoolean workerDrained = new AtomicBoolean();
    private final AtomicLong executing = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong cancelled = new AtomicLong();
    private final AtomicLong returned = new AtomicLong();
    private volatile AcceptedTask<?> currentTask;
    private volatile GroupLifecycleCoordinator groupOwner;

    private EventLoopKernel(
            EventLoop owner,
            String name,
            CapacityMode capacityMode,
            OptionalLong capacityLimit,
            TaskQueue queue,
            TaskAdmissionGate gate,
            ThreadFactory threadFactory,
            Duration shutdownTimeout,
            NanoClock clock,
            int maxCommandBatchSize,
            int maxTimerBatchSize,
            TaskExceptionHandler taskExceptionHandler,
            List<EventLoopModule> modules) {
        this.owner = Objects.requireNonNull(owner, "owner 不能为空");
        this.name = Objects.requireNonNull(name, "name 不能为空");
        this.capacityMode = Objects.requireNonNull(capacityMode, "capacityMode 不能为空");
        this.capacityLimit = Objects.requireNonNull(capacityLimit, "capacityLimit 不能为空");
        this.queue = Objects.requireNonNull(queue, "queue 不能为空");
        this.gate = Objects.requireNonNull(gate, "gate 不能为空");
        this.modules = new ModuleLifecycle(modules);
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.maxCommandBatchSize = requirePositive(maxCommandBatchSize,
                "maxCommandBatchSize");
        this.maxTimerBatchSize = requirePositive(maxTimerBatchSize,
                "maxTimerBatchSize");
        this.taskExceptionHandler = Objects.requireNonNull(
                taskExceptionHandler, "taskExceptionHandler 不能为空");
        EventLoopShutdownBackend shutdownBackend = new EventLoopShutdownBackend(this);
        this.supervisor = WorkerSupervisor.builder()
                .name(name)
                .shutdownTimeout(shutdownTimeout)
                .shutdownBackend(shutdownBackend)
                .build();
        Runnable supervised = supervisor.supervise(new EventLoopWorker(this));
        this.worker = Objects.requireNonNull(threadFactory, "threadFactory 不能为空")
                .newThread(supervised);
        if (worker == null) {
            throw new IllegalStateException("threadFactory 不能返回 null");
        }
        if (worker.getState() != Thread.State.NEW) {
            throw new IllegalStateException("threadFactory 必须返回尚未启动的线程：" + worker.getName());
        }
        supervisor.register(worker);
        supervisor.termination().whenComplete(this::publishTermination);
    }

    public static EventLoopKernel bounded(
            EventLoop owner,
            String name,
            int capacity,
            ThreadFactory threadFactory,
            Duration shutdownTimeout,
            NanoClock clock,
            int maxCommandBatchSize,
            int maxTimerBatchSize,
            TaskExceptionHandler taskExceptionHandler,
            List<EventLoopModule> modules) {
        return new EventLoopKernel(
                owner,
                name,
                CapacityMode.BOUNDED,
                OptionalLong.of(capacity),
                new BoundedTaskQueue(capacity),
                TaskAdmissionGate.bounded(capacity),
                threadFactory,
                shutdownTimeout,
                clock,
                maxCommandBatchSize,
                maxTimerBatchSize,
                taskExceptionHandler,
                List.copyOf(modules));
    }

    public static EventLoopKernel unbounded(
            EventLoop owner,
            String name,
            int segmentSize,
            ThreadFactory threadFactory,
            Duration shutdownTimeout,
            NanoClock clock,
            int maxCommandBatchSize,
            int maxTimerBatchSize,
            TaskExceptionHandler taskExceptionHandler,
            List<EventLoopModule> modules) {
        return new EventLoopKernel(
                owner,
                name,
                CapacityMode.UNBOUNDED,
                OptionalLong.empty(),
                new UnboundedTaskQueue(segmentSize),
                TaskAdmissionGate.unbounded(),
                threadFactory,
                shutdownTimeout,
                clock,
                maxCommandBatchSize,
                maxTimerBatchSize,
                taskExceptionHandler,
                List.copyOf(modules));
    }

    public String name() {
        return name;
    }

    public synchronized void bindOwner(GroupLifecycleCoordinator groupOwner) {
        Objects.requireNonNull(groupOwner, "groupOwner 不能为空");
        if (this.groupOwner != null) {
            throw new IllegalStateException("EventLoopKernel 只能绑定一个 Group owner");
        }
        if (supervisor.snapshot().lifecycle() != SupervisedLifecycle.NEW) {
            throw new IllegalStateException("只能在 NEW 状态绑定 Group owner");
        }
        this.groupOwner = groupOwner;
    }

    public CompletionStage<Void> start() {
        if (!startRequested.compareAndSet(false, true)) {
            return startupView;
        }
        boolean markedStarting = false;
        Throwable failure = null;
        try {
            supervisor.markStarting();
            markedStarting = true;
            worker.start();
        } catch (Throwable startFailure) {
            failure = startFailure;
            reportOwnerFailure(startFailure);
            supervisor.fail(startFailure);
            publishStartupFailure(startFailure);
        } finally {
            if (markedStarting) {
                try {
                    supervisor.sealWorkers();
                } catch (Throwable sealFailure) {
                    if (failure == null) {
                        reportOwnerFailure(sealFailure);
                        supervisor.fail(sealFailure);
                        publishStartupFailure(sealFailure);
                    } else if (failure != sealFailure) {
                        failure.addSuppressed(sealFailure);
                    }
                }
            }
        }
        return startupView;
    }

    public CompletionStage<EventLoopSnapshot> termination() {
        return terminationView;
    }

    public EventLoopSnapshot snapshot() {
        while (true) {
            boolean accepting = acceptingTasks();
            WorkerSnapshot workerSnapshot = supervisor.snapshot();
            if (!accepting || workerSnapshot.lifecycle() == SupervisedLifecycle.RUNNING) {
                return snapshot(workerSnapshot, accepting);
            }
            Thread.onSpinWait();
        }
    }

    public boolean inEventLoop() {
        return Thread.currentThread() == worker;
    }

    public boolean tryExecute(Runnable command) {
        Objects.requireNonNull(command, "command 不能为空");
        return submitOrdinary(command, () -> {
            command.run();
            return null;
        }, true) != null;
    }

    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command 不能为空");
        if (submitOrdinary(command, () -> {
            command.run();
            return null;
        }, true) == null) {
            throw rejected();
        }
    }

    public Future<?> submit(Runnable task) {
        return submit(task, null);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        Objects.requireNonNull(task, "task 不能为空");
        EventLoopFutureTask<T> future = submitOrdinary(task, () -> {
            task.run();
            return result;
        }, false);
        if (future == null) {
            throw rejected();
        }
        return future;
    }

    public <T> Future<T> submit(Callable<T> task) {
        Objects.requireNonNull(task, "task 不能为空");
        EventLoopFutureTask<T> future = admit((sequence, acceptedAt) -> {
            EventLoopFutureTask<T> createdFuture = new EventLoopFutureTask<>(
                    task, clock, initialSnapshot(sequence, acceptedAt), this::inEventLoop);
            AcceptedTask<T> accepted = new AcceptedTask<>(
                    sequence, createdFuture, createdFuture, null, false);
            bindCancellation(accepted);
            return accepted;
        });
        if (future == null) {
            throw rejected();
        }
        return future;
    }

    public <V> EventLoopScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec) {
        Objects.requireNonNull(spec, "spec 不能为空");
        return submitScheduled(spec, null);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        Objects.requireNonNull(command, "command 不能为空");
        Objects.requireNonNull(unit, "unit 不能为空");
        ScheduledTaskSpec<Void> spec = ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    command.run();
                    return null;
                })
                .triggerAfter(nonNegativeDuration(delay, unit))
                .build();
        return submitScheduled(spec, command);
    }

    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        Objects.requireNonNull(callable, "callable 不能为空");
        Objects.requireNonNull(unit, "unit 不能为空");
        ScheduledTaskSpec<V> spec = ScheduledTaskSpec.<V>builder()
                .task(context -> callable.call())
                .triggerAfter(nonNegativeDuration(delay, unit))
                .build();
        return submitScheduled(spec, null);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit) {
        Objects.requireNonNull(command, "command 不能为空");
        Objects.requireNonNull(unit, "unit 不能为空");
        ScheduledTaskSpec<Void> spec = ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    command.run();
                    return null;
                })
                .triggerAfter(nonNegativeDuration(initialDelay, unit))
                .scheduleMode(ScheduleMode.FIXED_RATE)
                .period(positiveDuration(period, unit, "period"))
                .build();
        return submitScheduled(spec, command);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command,
            long initialDelay,
            long delay,
            TimeUnit unit) {
        Objects.requireNonNull(command, "command 不能为空");
        Objects.requireNonNull(unit, "unit 不能为空");
        ScheduledTaskSpec<Void> spec = ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    command.run();
                    return null;
                })
                .triggerAfter(nonNegativeDuration(initialDelay, unit))
                .scheduleMode(ScheduleMode.FIXED_DELAY)
                .period(positiveDuration(delay, unit, "delay"))
                .build();
        return submitScheduled(spec, command);
    }

    public void shutdown() {
        requestShutdown(ShutdownMode.GRACEFUL, ShutdownDeadline.unbounded());
    }

    public List<Runnable> shutdownNow() {
        return shutdownNow(ShutdownDeadline.unbounded());
    }

    public List<Runnable> shutdownNow(ShutdownDeadline deadline) {
        Objects.requireNonNull(deadline, "deadline 不能为空");
        gate.closeForAdmissions();
        publishStartupFailure(new StartupAbortedException(name));
        awaitAdmissionsUninterruptibly();
        List<Runnable> notStarted = registry.sweepShutdownNow();
        returned.addAndGet(notStarted.size());
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE, deadline);
        LockSupport.unpark(worker);
        return notStarted;
    }

    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        Objects.requireNonNull(mode, "mode 不能为空");
        Objects.requireNonNull(deadline, "deadline 不能为空");
        gate.closeForAdmissions();
        publishStartupFailure(new StartupAbortedException(name));
        if (mode == ShutdownMode.IMMEDIATE) {
            registry.cancelAll(CancellationReason.SHUTDOWN_NOW);
        }
        supervisor.requestShutdown(mode, deadline);
        LockSupport.unpark(worker);
    }

    public boolean isShutdown() {
        SupervisedLifecycle lifecycle = supervisor.snapshot().lifecycle();
        return lifecycle == SupervisedLifecycle.QUIESCING
                || lifecycle == SupervisedLifecycle.STOPPING
                || lifecycle == SupervisedLifecycle.TERMINATED;
    }

    public boolean isTerminated() {
        return supervisor.snapshot().lifecycle() == SupervisedLifecycle.TERMINATED;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit 不能为空");
        if (inEventLoop()) {
            throw new BlockingOperationException("不能在所属 EventLoop 线程等待终止");
        }
        try {
            termination.toCompletableFuture().get(Math.max(0, timeout), unit);
            return true;
        } catch (TimeoutException ignored) {
            return false;
        } catch (ExecutionException failure) {
            throw new CompletionException(failure.getCause());
        }
    }

    public void close() {
        if (inEventLoop()) {
            throw new BlockingOperationException("不能在所属 EventLoop 线程关闭并等待自身");
        }
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

    void runWorker() {
        Throwable primaryFailure = null;
        boolean modulesStarted = false;
        try {
            supervisor.workersStarted().toCompletableFuture().join();
            modules.start(owner);
            modulesStarted = true;
            supervisor.markRunning();
            gate.open();
            publishStartupSuccess();
            runLoop();
        } catch (Throwable failure) {
            primaryFailure = unwrap(failure);
            gate.closeForAdmissions();
            publishStartupFailure(primaryFailure);
            reportOwnerFailure(primaryFailure);
            supervisor.fail(primaryFailure);
        } finally {
            gate.closeForAdmissions();
            cancelAndClearRetainedTasks();
            Throwable stopFailure = modulesStarted || modules.startedCount() > 0
                    ? modules.stop(owner, primaryFailure)
                    : primaryFailure;
            if (stopFailure != null && stopFailure != primaryFailure) {
                reportOwnerFailure(stopFailure);
                supervisor.fail(stopFailure);
                primaryFailure = stopFailure;
            }
        }
    }

    void beginQuiesce() {
        gate.closeForAdmissions();
        quiescing.set(true);
        workerDrained.set(false);
        LockSupport.unpark(worker);
    }

    boolean workerDrained() {
        return workerDrained.get();
    }

    void stopWorker(ShutdownMode mode) {
        stopMode.accumulateAndGet(mode, (current, requested) ->
                current == ShutdownMode.IMMEDIATE || requested == ShutdownMode.IMMEDIATE
                        ? ShutdownMode.IMMEDIATE
                        : ShutdownMode.GRACEFUL);
        LockSupport.unpark(worker);
    }

    private void runLoop() throws Throwable {
        while (stopMode.get() == null) {
            boolean didWork = processCancellations();
            if (quiescing.get()) {
                didWork |= cancelWaitingPeriodicTasks();
            }
            didWork |= processDueTimers();
            didWork |= processCommands();
            if (didWork) {
                modules.update(owner, clock.nanoTime());
            }
            publishDrainFact();
            if (!didWork && stopMode.get() == null) {
                parkUntilWork();
            }
        }
    }

    private boolean processCancellations() {
        boolean processed = false;
        AcceptedTask<?> task;
        while ((task = cancellationMailbox.poll()) != null) {
            processed = true;
            if (task.scheduled() && timers.remove(task.scheduledTask())) {
                terminateTask(task);
            }
        }
        return processed;
    }

    private boolean cancelWaitingPeriodicTasks() {
        if (timers.isEmpty()) {
            return false;
        }
        boolean changed = false;
        List<ScheduledTask<?>> retained = new ArrayList<>(timers.size());
        ScheduledTask<?> scheduled;
        while ((scheduled = timers.poll()) != null) {
            AcceptedTask<?> task = scheduled.acceptedTask();
            if (scheduled.isPeriodic()) {
                registry.cancelWaiting(task, CancellationReason.SHUTDOWN);
                terminateTask(task);
                changed = true;
            } else {
                retained.add(scheduled);
            }
        }
        retained.forEach(timers::add);
        return changed;
    }

    private boolean processDueTimers() throws Throwable {
        boolean processed = false;
        for (int count = 0; count < maxTimerBatchSize; count++) {
            ScheduledTask<?> scheduled = timers.peek();
            if (scheduled == null || !scheduled.isDue(clock.nanoTime())) {
                break;
            }
            timers.poll();
            AcceptedTask<?> task = scheduled.acceptedTask();
            if (!task.tryStart()) {
                terminateTask(task);
                processed = true;
                continue;
            }
            processed = true;
            executing.incrementAndGet();
            boolean repeat;
            currentTask = task;
            try {
                repeat = scheduled.runInvocation();
            } finally {
                executing.decrementAndGet();
            }
            if (repeat && !quiescing.get() && task.returnToWaiting()) {
                timers.add(scheduled);
            } else {
                if (repeat && !task.future().isDone()) {
                    task.future().cancel(CancellationReason.SHUTDOWN);
                }
                terminateTask(task);
            }
            currentTask = null;
        }
        return processed;
    }

    private boolean processCommands() {
        boolean processed = false;
        for (int count = 0; count < maxCommandBatchSize; count++) {
            AcceptedTask<?> task = queue.poll();
            if (task == null) {
                break;
            }
            processed = true;
            if (task.state() != AcceptedTask.PhysicalState.WAITING
                    || task.future().isDone()) {
                terminateTask(task);
                continue;
            }
            if (task.scheduled()) {
                if (quiescing.get() && task.scheduledTask().isPeriodic()) {
                    registry.cancelWaiting(task, CancellationReason.SHUTDOWN);
                    terminateTask(task);
                    continue;
                }
                timers.add(task.scheduledTask());
                if (task.scheduledTask().isDue(clock.nanoTime())) {
                    break;
                }
                continue;
            }
            runOrdinary(task);
        }
        return processed;
    }

    private void runOrdinary(AcceptedTask<?> task) {
        if (!task.tryStart()) {
            terminateTask(task);
            return;
        }
        executing.incrementAndGet();
        currentTask = task;
        try {
            task.future().run();
        } finally {
            executing.decrementAndGet();
        }
        Throwable taskFailure = task.future().failure();
        if (task.reportFailure() && taskFailure != null) {
            try {
                taskExceptionHandler.handle(owner, task.originalRunnable(), taskFailure);
            } finally {
                terminateTask(task);
                currentTask = null;
            }
            return;
        }
        terminateTask(task);
        currentTask = null;
    }

    private void publishDrainFact() {
        if (!quiescing.get()) {
            return;
        }
        workerDrained.set(gate.activeAdmissions() == 0
                && queue.pending() == 0
                && timers.isEmpty()
                && cancellationMailbox.isEmpty()
                && executing.get() == 0
                && registry.size() == 0);
    }

    private void parkUntilWork() {
        ScheduledTask<?> next = timers.peek();
        if (next == null) {
            LockSupport.park(this);
        } else {
            long remaining = next.triggerNanos() - clock.nanoTime();
            if (remaining > 0) {
                LockSupport.parkNanos(this, remaining);
            }
        }
        if (Thread.interrupted() && stopMode.get() == null) {
            // 外部中断不是生命周期信号；清除状态后继续由 supervisor 管理。
        }
    }

    private <T> EventLoopFutureTask<T> submitOrdinary(
            Runnable original,
            Callable<T> callable,
            boolean reportFailure) {
        Objects.requireNonNull(original, "original 不能为空");
        Objects.requireNonNull(callable, "callable 不能为空");
        return admit((sequence, acceptedAt) -> {
            EventLoopFutureTask<T> future = new EventLoopFutureTask<>(
                    callable, clock, initialSnapshot(sequence, acceptedAt), this::inEventLoop);
            AcceptedTask<T> task = new AcceptedTask<>(
                    sequence, original, future, null, reportFailure);
            bindCancellation(task);
            return task;
        });
    }

    private <V> EventLoopScheduledFuture<V> submitScheduled(
            ScheduledTaskSpec<V> spec,
            Runnable original) {
        EventLoopFutureTask<V> future = admit((sequence, acceptedAt) -> {
            AtomicReference<Runnable> originalRef = new AtomicReference<>();
            ScheduledTask<V> scheduled = new ScheduledTask<>(
                    sequence,
                    spec,
                    acceptedAt,
                    clock,
                    failure -> taskExceptionHandler.handle(owner, originalRef.get(), failure),
                    this::inEventLoop);
            EventLoopFutureTask<V> createdFuture = scheduled.future();
            Runnable originalRunnable = original == null ? createdFuture : original;
            originalRef.set(originalRunnable);
            AcceptedTask<V> task = new AcceptedTask<>(
                    sequence, originalRunnable, createdFuture, scheduled, false);
            scheduled.bind(task);
            bindCancellation(task);
            return task;
        });
        if (future == null) {
            throw rejected();
        }
        return future;
    }

    private <T> EventLoopFutureTask<T> admit(TaskFactory<T> factory) {
        GroupLifecycleCoordinator.AdmissionLease ownerLease = acquireOwnerAdmission();
        if (groupOwner != null && ownerLease == null) {
            return null;
        }
        try {
            AdmissionToken token = gate.tryAcquire();
            if (token == null) {
                return null;
            }
            TaskReservation reservation = queue.tryReserve();
            if (reservation == null) {
                token.abort();
                return null;
            }
            AcceptedTask<T> task = null;
            boolean registered = false;
            try {
                long acceptedAt = clock.nanoTime();
                task = factory.create(reservation.sequence(), acceptedAt);
                registry.register(task, token);
                registered = true;
                reservation.publish(task);
                token.commit();
                LockSupport.unpark(worker);
                return task.future();
            } catch (Throwable failure) {
                reservation.abort();
                if (registered) {
                    registry.rollback(task);
                }
                token.abort();
                throw failure;
            }
        } finally {
            if (ownerLease != null) {
                ownerLease.close();
            }
        }
    }

    private GroupLifecycleCoordinator.AdmissionLease acquireOwnerAdmission() {
        GroupLifecycleCoordinator current = groupOwner;
        return current == null ? null : current.tryAcquireAdmission();
    }

    private boolean acceptingTasks() {
        if (!gate.accepting()) {
            return false;
        }
        GroupLifecycleCoordinator current = groupOwner;
        return current == null || current.snapshot().accepting();
    }

    private void reportOwnerFailure(Throwable failure) {
        GroupLifecycleCoordinator current = groupOwner;
        if (current != null) {
            current.childFailed(owner, failure);
        }
    }

    private void bindCancellation(AcceptedTask<?> task) {
        task.future().setCancellationAction(() -> {
            task.markCancelledWaiting();
            if (task.state() == AcceptedTask.PhysicalState.RUNNING
                    && task.future().snapshot().cancellationReason().interruptRequested()) {
                worker.interrupt();
            }
            cancellationMailbox.offer(task);
            LockSupport.unpark(worker);
        });
    }

    private void terminateTask(AcceptedTask<?> task) {
        if (!registry.terminate(task)) {
            return;
        }
        switch (task.future().snapshot().outcome()) {
            case SUCCEEDED -> completed.incrementAndGet();
            case FAILED -> failed.incrementAndGet();
            case CANCELLED -> cancelled.incrementAndGet();
            case WAITING, RUNNING -> {
                task.future().cancel(CancellationReason.SHUTDOWN_NOW);
                cancelled.incrementAndGet();
            }
        }
    }

    private void cancelAndClearRetainedTasks() {
        registry.cancelAll(CancellationReason.SHUTDOWN_NOW);
        AcceptedTask<?> running = currentTask;
        if (running != null) {
            running.future().cancel(CancellationReason.SHUTDOWN_NOW);
            terminateTask(running);
            currentTask = null;
        }
        ScheduledTask<?> scheduled;
        while ((scheduled = timers.poll()) != null) {
            terminateTask(scheduled.acceptedTask());
        }
        AcceptedTask<?> task;
        while ((task = queue.poll()) != null) {
            terminateTask(task);
        }
        while (cancellationMailbox.poll() != null) {
            // 任务已由 timer/queue/running 路径完成物理清理。
        }
        workerDrained.set(registry.size() == 0 && gate.activeAdmissions() == 0);
    }

    private void awaitAdmissionsUninterruptibly() {
        boolean interrupted = false;
        while (true) {
            try {
                gate.awaitAdmissions(ShutdownDeadline.unbounded());
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private EventLoopSnapshot snapshot(WorkerSnapshot workerSnapshot, boolean accepting) {
        return EventLoopSnapshot.builder()
                .name(name)
                .lifecycle(workerSnapshot.lifecycle())
                .acceptingTasks(accepting)
                .capacityMode(capacityMode)
                .capacityLimit(capacityLimit)
                .outstandingTasks(gate.outstanding())
                .ingressPendingTasks(queue.pending())
                .scheduledPendingTasks(timers.size())
                .executingTasks(executing.get())
                .completedTasks(completed.get())
                .failedTasks(failed.get())
                .cancelledTasks(cancelled.get())
                .shutdownNowReturnedTasks(returned.get())
                .allocatedQueueSegments(queue.allocatedSegments())
                .failure(workerSnapshot.failure())
                .shutdownMode(workerSnapshot.shutdownMode())
                .worker(workerSnapshot)
                .build();
    }

    private ScheduledTaskSnapshot initialSnapshot(long sequence, long acceptedAt) {
        return ScheduledTaskSnapshot.builder()
                .acceptedSequence(sequence)
                .scheduleMode(ScheduleMode.ONE_SHOT)
                .triggerNanos(acceptedAt)
                .expiresAtNanos(OptionalLong.empty())
                .priority(0)
                .executions(0)
                .maxExecutions(OptionalInt.empty())
                .started(false)
                .outcome(TaskOutcome.WAITING)
                .build();
    }

    private void publishStartupSuccess() {
        if (startupOutcomeClaimed.compareAndSet(false, true)) {
            Thread.ofVirtual().name(name + "-startup-notifier").start(() -> startup.complete(null));
        }
    }

    private void publishStartupFailure(Throwable failure) {
        if (startupOutcomeClaimed.compareAndSet(false, true)) {
            Thread.ofVirtual().name(name + "-startup-notifier")
                    .start(() -> startup.completeExceptionally(failure));
        }
    }

    private void publishTermination(WorkerSnapshot workerSnapshot, Throwable stageFailure) {
        Thread.ofVirtual().name(name + "-termination-notifier").start(() -> {
            if (stageFailure != null) {
                termination.completeExceptionally(stageFailure);
            } else {
                termination.complete(snapshot(workerSnapshot, false));
            }
        });
    }

    private RejectedExecutionException rejected() {
        EventLoopSnapshot snapshot = snapshot();
        return new RejectedExecutionException("EventLoop 拒绝任务：name=" + name
                + "，lifecycle=" + snapshot.lifecycle()
                + "，accepting=" + snapshot.acceptingTasks()
                + "，outstanding=" + snapshot.outstandingTasks());
    }

    private static Duration nonNegativeDuration(long value, TimeUnit unit) {
        return Duration.ofNanos(Math.max(0L, unit.toNanos(value)));
    }

    private static Duration positiveDuration(long value, TimeUnit unit, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " 必须为正数，实际值=" + value);
        }
        return Duration.ofNanos(unit.toNanos(value));
    }

    private static int requirePositive(int value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " 必须为正数，实际值=" + value);
        }
        return value;
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    @FunctionalInterface
    private interface TaskFactory<T> {
        AcceptedTask<T> create(long sequence, long acceptedAtNanos);
    }

    private static final class StartupAbortedException extends IllegalStateException {
        private StartupAbortedException(String name) {
            super("EventLoop 启动在完成前被关闭：" + name);
        }
    }
}
