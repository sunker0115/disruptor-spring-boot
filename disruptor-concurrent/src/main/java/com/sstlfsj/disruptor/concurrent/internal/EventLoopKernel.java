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
    private final TaskDispositionCoordinator disposition = new TaskDispositionCoordinator();
    private final AtomicBoolean discardExecutionStarted = new AtomicBoolean();
    private final CompletableFuture<Void> discardExecution = new CompletableFuture<>();
    private final AtomicBoolean workerDrained = new AtomicBoolean();
    private volatile long executing;
    private volatile long completed;
    private volatile long failed;
    private volatile long cancelled;
    private final AtomicLong returned = new AtomicLong();
    private final WorkerWakeup wakeup;
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
        this.wakeup = new WorkerWakeup(gate, () -> LockSupport.unpark(worker));
        disposition.completion().whenComplete((ignored, failure) -> {
            if (failure != null) {
                reportDispositionFailure(unwrap(failure));
            }
        });
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
        return admitOrdinary(command);
    }

    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command 不能为空");
        if (!admitOrdinary(command)) {
            throw rejected();
        }
    }

    public Future<?> submit(Runnable task) {
        return submit(task, null);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        Objects.requireNonNull(task, "task 不能为空");
        EventLoopFutureTask<T> future = admitTracked(TaskType.TRACKED,
                (sequence, acceptedAt) -> {
                    EventLoopFutureTask<T> createdFuture = new EventLoopFutureTask<>(
                            () -> {
                                task.run();
                                return result;
                            }, clock, initialSnapshot(sequence, acceptedAt), this::inEventLoop);
                    AcceptedTask<T> accepted = AcceptedTask.<T>builder()
                            .acceptedSequence(sequence)
                            .shutdownNowReturnValue(task)
                            .future(createdFuture)
                            .build();
                    bindCancellation(accepted);
                    return accepted;
                });
        if (future == null) {
            throw rejected();
        }
        return future;
    }

    public <T> Future<T> submit(Callable<T> task) {
        Objects.requireNonNull(task, "task 不能为空");
        EventLoopFutureTask<T> future = admitTracked(TaskType.TRACKED, (sequence, acceptedAt) -> {
            EventLoopFutureTask<T> createdFuture = new EventLoopFutureTask<>(
                    task, clock, initialSnapshot(sequence, acceptedAt), this::inEventLoop);
            AcceptedTask<T> accepted = AcceptedTask.<T>builder()
                    .acceptedSequence(sequence)
                    .shutdownNowReturnValue(createdFuture)
                    .future(createdFuture)
                    .build();
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
        boolean returning = disposition.tryBeginReturning();
        try {
            supervisor.requestShutdown(ShutdownMode.IMMEDIATE, deadline);
            if (!returning) {
                return List.of();
            }
            gate.awaitDrained();
            List<Runnable> notStarted = returnUnstarted();
            returned.addAndGet(notStarted.size());
            disposition.completeReturned();
            return notStarted;
        } catch (Throwable failure) {
            if (returning) {
                disposition.failReturningToDiscarding(failure);
                startDiscardExecution();
            }
            throw failure;
        } finally {
            LockSupport.unpark(worker);
        }
    }

    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        Objects.requireNonNull(mode, "mode 不能为空");
        Objects.requireNonNull(deadline, "deadline 不能为空");
        gate.closeForAdmissions();
        publishStartupFailure(new StartupAbortedException(name));
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
            if (stopMode.get() != ShutdownMode.GRACEFUL) {
                disposition.tryBeginDiscarding();
            }
            awaitTaskDisposition();
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
        gate.closeForAdmissions();
        stopMode.accumulateAndGet(mode, (current, requested) ->
                current == ShutdownMode.IMMEDIATE || requested == ShutdownMode.IMMEDIATE
                        ? ShutdownMode.IMMEDIATE
                        : ShutdownMode.GRACEFUL);
        if (mode == ShutdownMode.IMMEDIATE) {
            disposition.tryBeginDiscarding();
            worker.interrupt();
        }
        LockSupport.unpark(worker);
        if (mode == ShutdownMode.IMMEDIATE
                && disposition.state() == TaskDispositionCoordinator.State.DISCARDING) {
            startDiscardExecution();
        }
    }

    private void runLoop() throws Throwable {
        while (canProcessTasks()) {
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
        int physicalCompleted = 0;
        AcceptedTask<?> task;
        try {
            while (canProcessTasks() && (task = cancellationMailbox.poll()) != null) {
                processed = true;
                if (task.scheduled() && timers.remove(task.scheduledTask())) {
                    physicalCompleted += terminateTask(task);
                }
            }
            return processed;
        } finally {
            if (physicalCompleted != 0) {
                gate.completeBatch(physicalCompleted);
            }
        }
    }

    private boolean cancelWaitingPeriodicTasks() {
        if (timers.isEmpty()) {
            return false;
        }
        boolean changed = false;
        int physicalCompleted = 0;
        List<ScheduledTask<?>> retained = new ArrayList<>(timers.size());
        ScheduledTask<?> scheduled;
        try {
            while (canProcessTasks() && (scheduled = timers.poll()) != null) {
                AcceptedTask<?> task = scheduled.acceptedTask();
                if (scheduled.isPeriodic()) {
                    if (task.markCancelledWaiting()) {
                        task.future().cancel(CancellationReason.SHUTDOWN);
                    }
                    physicalCompleted += terminateTask(task);
                    changed = true;
                } else {
                    retained.add(scheduled);
                }
            }
            retained.forEach(timers::add);
            return changed;
        } finally {
            if (physicalCompleted != 0) {
                gate.completeBatch(physicalCompleted);
            }
        }
    }

    private boolean processDueTimers() throws Throwable {
        boolean processed = false;
        int physicalCompleted = 0;
        try {
            for (int count = 0; count < maxTimerBatchSize; count++) {
                if (!canProcessTasks()) {
                    break;
                }
                ScheduledTask<?> scheduled = timers.peek();
                if (scheduled == null || !scheduled.isDue(clock.nanoTime())) {
                    break;
                }
                timers.poll();
                AcceptedTask<?> task = scheduled.acceptedTask();
                processed = true;
                if (!task.tryStart()) {
                    physicalCompleted += terminateTask(task);
                    continue;
                }
                executing++;
                currentTask = task;
                try {
                    boolean repeat = scheduled.runInvocation();
                    if (task.returnToWaiting(() -> scheduled.canRearm(
                            repeat, quiescing.get() || !canProcessTasks()))) {
                        timers.add(scheduled);
                    } else {
                        if (repeat && !task.future().isDone()) {
                            task.future().cancel(CancellationReason.SHUTDOWN);
                        }
                        physicalCompleted += terminateTask(task);
                    }
                } catch (Throwable failure) {
                    if (!task.future().isDone()) {
                        task.future().cancel(CancellationReason.SHUTDOWN_NOW);
                    }
                    physicalCompleted += terminateTask(task);
                    throw failure;
                } finally {
                    currentTask = null;
                    executing--;
                }
            }
            return processed;
        } finally {
            if (physicalCompleted != 0) {
                gate.completeBatch(physicalCompleted);
            }
        }
    }

    private boolean processCommands() throws Throwable {
        boolean processed = false;
        int physicalCompleted = 0;
        try {
            for (int count = 0; count < maxCommandBatchSize; count++) {
                if (!canProcessTasks() || !queue.poll()) {
                    break;
                }
                processed = true;
                switch (queue.currentType()) {
                    case TOMBSTONE -> {
                        queue.advanceConsumer();
                        queue.releaseCurrentSlot();
                    }
                    case ORDINARY -> {
                        if (!queue.tryStartCurrentOrdinary()) {
                            OrdinaryState state = queue.currentOrdinaryState();
                            awaitTaskDisposition();
                            queue.advanceConsumer();
                            queue.releaseCurrentSlot();
                            if (state == OrdinaryState.RETURNED
                                    || state == OrdinaryState.DISCARDED) {
                                cancelled++;
                            }
                            physicalCompleted++;
                            continue;
                        }
                        Runnable ordinary = queue.currentOrdinary();
                        queue.advanceConsumer();
                        executing++;
                        try {
                            ordinary.run();
                            completed++;
                        } catch (Throwable failure) {
                            failed++;
                            taskExceptionHandler.handle(owner, ordinary, failure);
                        } finally {
                            executing--;
                            queue.terminalizeCurrentOrdinary();
                            queue.releaseCurrentSlot();
                            physicalCompleted++;
                        }
                    }
                    case TRACKED -> {
                        AcceptedTask<?> task = queue.currentRecord();
                        queue.advanceConsumer();
                        queue.releaseCurrentSlot();
                        if (task.future().isDone() || !task.tryStart()) {
                            physicalCompleted += terminateTask(task);
                            continue;
                        }
                        executing++;
                        currentTask = task;
                        try {
                            task.future().run();
                        } finally {
                            currentTask = null;
                            executing--;
                        }
                        physicalCompleted += terminateTask(task);
                    }
                    case SCHEDULE -> {
                        AcceptedTask<?> task = queue.currentRecord();
                        queue.advanceConsumer();
                        queue.releaseCurrentSlot();
                        if (task.state() != AcceptedTask.PhysicalState.WAITING
                                || task.future().isDone()) {
                            physicalCompleted += terminateTask(task);
                            continue;
                        }
                        ScheduledTask<?> scheduled = task.scheduledTask();
                        if (quiescing.get() && scheduled.isPeriodic()) {
                            if (task.markCancelledWaiting()) {
                                task.future().cancel(CancellationReason.SHUTDOWN);
                            }
                            physicalCompleted += terminateTask(task);
                            continue;
                        }
                        timers.add(scheduled);
                        if (scheduled.isDue(clock.nanoTime())) {
                            return true;
                        }
                    }
                }
            }
            return processed;
        } finally {
            if (physicalCompleted != 0) {
                gate.completeBatch(physicalCompleted);
            }
        }
    }

    private void publishDrainFact() {
        if (!quiescing.get()) {
            return;
        }
        workerDrained.set(gate.activePublishers() == 0
                && queue.pending() == 0
                && timers.isEmpty()
                && cancellationMailbox.isEmpty()
                && executing == 0
                && registry.size() == 0);
    }

    private boolean canProcessTasks() {
        return stopMode.get() == null
                && disposition.state() == TaskDispositionCoordinator.State.NONE;
    }

    private void parkUntilWork() {
        wakeup.prepareToPark();
        ScheduledTask<?> next = timers.peek();
        if (canProcessTasks()
                && cancellationMailbox.isEmpty()
                && !queue.poll()
                && (next == null || !next.isDue(clock.nanoTime()))) {
            if (next == null) {
                LockSupport.park(this);
            } else {
                long remaining = next.triggerNanos() - clock.nanoTime();
                if (remaining > 0) {
                    LockSupport.parkNanos(this, remaining);
                }
            }
        }
        wakeup.awake();
        if (Thread.interrupted() && stopMode.get() == null) {
            // 外部中断不是生命周期信号；清除状态后继续由 supervisor 管理。
        }
    }

    private <V> EventLoopScheduledFuture<V> submitScheduled(
            ScheduledTaskSpec<V> spec,
            Runnable original) {
        EventLoopFutureTask<V> future = admitTracked(TaskType.SCHEDULE, (sequence, acceptedAt) -> {
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
            AcceptedTask<V> task = AcceptedTask.<V>builder()
                    .acceptedSequence(sequence)
                    .shutdownNowReturnValue(originalRunnable)
                    .future(createdFuture)
                    .scheduledTask(scheduled)
                    .build();
            scheduled.bind(task);
            bindCancellation(task);
            return task;
        });
        if (future == null) {
            throw rejected();
        }
        return future;
    }

    private boolean admitOrdinary(Runnable command) {
        GroupLifecycleCoordinator.AdmissionLease ownerLease = acquireOwnerAdmission();
        if (groupOwner != null && ownerLease == null) {
            return false;
        }
        try {
            if (!gate.tryEnter()) {
                return false;
            }
            boolean rollbackOutstanding = true;
            long sequence = -1;
            try {
                try {
                    try {
                        sequence = queue.tryClaim();
                        if (sequence < 0) {
                            return false;
                        }
                        queue.writeOrdinary(sequence, command);
                        rollbackOutstanding = false;
                    } finally {
                        if (sequence >= 0 && rollbackOutstanding) {
                            queue.writeTombstone(sequence);
                        }
                    }
                } finally {
                    if (sequence >= 0) {
                        queue.publish(sequence);
                    }
                }
            } finally {
                wakeup.finishAdmission(sequence, rollbackOutstanding);
            }
            return true;
        } finally {
            if (ownerLease != null) {
                ownerLease.close();
            }
        }
    }

    private <T> EventLoopFutureTask<T> admitTracked(
            TaskType type,
            TaskFactory<T> factory) {
        if (type != TaskType.TRACKED && type != TaskType.SCHEDULE) {
            throw new IllegalArgumentException("tracked 准入类型非法：" + type);
        }
        Objects.requireNonNull(factory, "factory 不能为空");
        GroupLifecycleCoordinator.AdmissionLease ownerLease = acquireOwnerAdmission();
        if (groupOwner != null && ownerLease == null) {
            return null;
        }
        try {
            if (!gate.tryEnter()) {
                return null;
            }
            boolean rollbackOutstanding = true;
            long sequence = -1;
            AcceptedTask<T> task = null;
            boolean registered = false;
            try {
                try {
                    try {
                        sequence = queue.tryClaim();
                        if (sequence < 0) {
                            return null;
                        }
                        task = factory.create(sequence, clock.nanoTime());
                        registry.register(task);
                        registered = true;
                        if (type == TaskType.TRACKED) {
                            queue.writeTracked(sequence, task);
                        } else {
                            queue.writeSchedule(sequence, task);
                        }
                        rollbackOutstanding = false;
                    } finally {
                        if (sequence >= 0 && rollbackOutstanding) {
                            if (registered) {
                                registry.terminalizeAndRemove(task);
                            }
                            queue.writeTombstone(sequence);
                        }
                    }
                } finally {
                    if (sequence >= 0) {
                        queue.publish(sequence);
                    }
                }
            } finally {
                wakeup.finishAdmission(sequence, rollbackOutstanding);
            }
            return task.future();
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
        if (!gate.isAccepting()) {
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

    private int terminateTask(AcceptedTask<?> task) {
        if (task.state() != AcceptedTask.PhysicalState.RUNNING) {
            awaitTaskDisposition();
        }
        if (!task.terminate()) {
            return 0;
        }
        registry.remove(task);
        switch (task.future().snapshot().outcome()) {
            case SUCCEEDED -> completed++;
            case FAILED -> failed++;
            case CANCELLED -> cancelled++;
            case WAITING, RUNNING -> {
                task.future().cancel(CancellationReason.SHUTDOWN_NOW);
                cancelled++;
            }
        }
        return 1;
    }

    private void cancelAndClearRetainedTasks() {
        gate.awaitDrained();
        int physicalCompleted = 0;
        AcceptedTask<?> running = currentTask;
        if (running != null) {
            running.future().cancel(CancellationReason.SHUTDOWN_NOW);
            physicalCompleted += terminateTask(running);
            currentTask = null;
        }
        ScheduledTask<?> scheduled;
        while ((scheduled = timers.poll()) != null) {
            physicalCompleted += terminateTask(scheduled.acceptedTask());
        }
        while (queue.poll()) {
            TaskType type = queue.currentType();
            if (type == TaskType.ORDINARY) {
                cancelled++;
                queue.advanceConsumer();
                queue.releaseCurrentSlot();
                physicalCompleted++;
            } else if (type == TaskType.TOMBSTONE) {
                queue.advanceConsumer();
                queue.releaseCurrentSlot();
            } else {
                AcceptedTask<?> task = queue.currentRecord();
                queue.advanceConsumer();
                queue.releaseCurrentSlot();
                physicalCompleted += terminateTask(task);
            }
        }
        while (cancellationMailbox.poll() != null) {
            // 任务已由 timer/queue/running 路径完成物理清理。
        }
        List<AcceptedTask<?>> retained = new ArrayList<>();
        registry.scanForShutdown(retained::add);
        for (AcceptedTask<?> task : retained) {
            physicalCompleted += terminateTask(task);
        }
        if (physicalCompleted != 0) {
            gate.completeBatch(physicalCompleted);
        }
        workerDrained.set(registry.size() == 0 && gate.activePublishers() == 0);
    }

    private List<Runnable> returnUnstarted() {
        List<SequencedRunnable> claimed = new ArrayList<>();
        long frozen = queue.claimedCursor();
        queue.scanOrdinaryUnstarted(frozen, OrdinaryDisposition.RETURN,
                (sequence, original) -> claimed.add(new SequencedRunnable(sequence, original)));
        registry.scanForShutdown(task -> {
            if (task.tryReturn()) {
                task.future().cancel(CancellationReason.SHUTDOWN_NOW);
                claimed.add(new SequencedRunnable(
                        task.acceptedSequence(), task.shutdownNowReturnValue()));
            } else if (task.state() == AcceptedTask.PhysicalState.RUNNING) {
                task.future().cancel(true);
            }
        });
        claimed.sort((left, right) -> Long.compare(left.sequence(), right.sequence()));
        return claimed.stream().map(SequencedRunnable::task).toList();
    }

    private void discardUnstarted() {
        gate.awaitDrained();
        long frozen = queue.claimedCursor();
        queue.scanOrdinaryUnstarted(frozen, OrdinaryDisposition.DISCARD,
                (sequence, original) -> {
                });
        registry.scanForShutdown(task -> {
            if (task.tryDiscard()
                    || task.state() == AcceptedTask.PhysicalState.DISCARDED
                    || task.state() == AcceptedTask.PhysicalState.RETURNED) {
                task.future().cancel(CancellationReason.SHUTDOWN_NOW);
            } else if (task.state() == AcceptedTask.PhysicalState.RUNNING) {
                task.future().cancel(true);
            }
        });
    }

    private void startDiscardExecution() {
        if (!discardExecutionStarted.compareAndSet(false, true)) {
            return;
        }
        try {
            Thread.ofVirtual().name(name + "-task-disposition").start(() -> {
                try {
                    discardUnstarted();
                    disposition.completeDiscarded();
                    discardExecution.complete(null);
                } catch (Throwable failure) {
                    reportDispositionFailure(failure);
                    discardExecution.completeExceptionally(failure);
                } finally {
                    LockSupport.unpark(worker);
                }
            });
        } catch (Throwable failure) {
            reportDispositionFailure(failure);
            discardExecution.completeExceptionally(failure);
            LockSupport.unpark(worker);
        }
    }

    private void awaitTaskDisposition() {
        boolean interrupted = Thread.interrupted();
        try {
            while (!disposition.isTerminal()) {
                if (disposition.state() == TaskDispositionCoordinator.State.NONE) {
                    return;
                }
                if (disposition.state() == TaskDispositionCoordinator.State.RETURNING) {
                    interrupted |= Thread.interrupted();
                    if (disposition.state() == TaskDispositionCoordinator.State.RETURNING) {
                        LockSupport.park(disposition);
                    }
                    continue;
                }
                startDiscardExecution();
                try {
                    discardUnstarted();
                    disposition.completeDiscarded();
                } catch (Throwable failure) {
                    reportDispositionFailure(failure);
                    awaitDiscardExecution();
                    if (!disposition.isTerminal()) {
                        discardUnstarted();
                        disposition.completeDiscarded();
                    }
                }
            }
            if (discardExecutionStarted.get()) {
                // 取消回调也必须完成，避免物理清理后又向 mailbox 写入引用。
                awaitDiscardExecution();
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void awaitDiscardExecution() {
        try {
            discardExecution.join();
        } catch (CompletionException ignored) {
            // 执行者已上报 supervisor；worker 仍接管未完成的物理处置。
        }
    }

    private void reportDispositionFailure(Throwable failure) {
        supervisor.fail(failure);
        reportOwnerFailure(failure);
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
                .executingTasks(executing)
                .completedTasks(completed)
                .failedTasks(failed)
                .cancelledTasks(cancelled)
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

    private record SequencedRunnable(long sequence, Runnable task) {
    }

    private static final class StartupAbortedException extends IllegalStateException {
        private StartupAbortedException(String name) {
            super("EventLoop 启动在完成前被关闭：" + name);
        }
    }
}
