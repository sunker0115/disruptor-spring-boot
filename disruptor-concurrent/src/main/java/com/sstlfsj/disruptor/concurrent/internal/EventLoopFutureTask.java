package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.BlockingOperationException;
import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.EventLoopScheduledFuture;
import com.sstlfsj.disruptor.concurrent.NanoClock;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;
import com.sstlfsj.disruptor.concurrent.TaskOutcome;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/** EventLoop 内部唯一的 Future 终态和同 loop 阻塞保护实现。 */
final class EventLoopFutureTask<V> extends FutureTask<V>
        implements EventLoopScheduledFuture<V> {

    private final Object completionLock = new Object();
    private final NanoClock clock;
    private final BooleanSupplier inEventLoop;
    private final AtomicReference<Runnable> terminalAction = new AtomicReference<>();
    private final AtomicBoolean terminalActionRun = new AtomicBoolean();

    private ScheduledTaskSnapshot snapshot;

    EventLoopFutureTask(
            Callable<V> callable,
            NanoClock clock,
            ScheduledTaskSnapshot initialSnapshot,
            BooleanSupplier inEventLoop) {
        super(Objects.requireNonNull(callable, "callable 不能为空"));
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.snapshot = Objects.requireNonNull(initialSnapshot, "initialSnapshot 不能为空");
        this.inEventLoop = Objects.requireNonNull(inEventLoop, "inEventLoop 不能为空");
    }

    EventLoopFutureTask(NanoClock clock, ScheduledTaskSnapshot initialSnapshot) {
        this(() -> null, clock, initialSnapshot, () -> false);
    }

    @Override
    public ScheduledTaskSnapshot snapshot() {
        synchronized (completionLock) {
            return snapshot;
        }
    }

    void updateNonTerminal(ScheduledTaskSnapshot nextSnapshot) {
        Objects.requireNonNull(nextSnapshot, "nextSnapshot 不能为空");
        if (nextSnapshot.outcome().isTerminal()) {
            throw new IllegalArgumentException("updateNonTerminal 不接收终态快照");
        }
        synchronized (completionLock) {
            if (!super.isDone()) {
                snapshot = nextSnapshot;
            }
        }
    }

    void setTerminalAction(Runnable action) {
        Objects.requireNonNull(action, "action 不能为空");
        if (!terminalAction.compareAndSet(null, action)) {
            throw new IllegalStateException("terminalAction 只能设置一次");
        }
        if (isDone()) {
            runTerminalAction();
        }
    }

    boolean completeSuccess(V value, ScheduledTaskSnapshot completedSnapshot) {
        synchronized (completionLock) {
            if (super.isDone()) {
                return false;
            }
            super.set(value);
            snapshot = requireOutcome(completedSnapshot, TaskOutcome.SUCCEEDED);
            return true;
        }
    }

    boolean completeFailure(Throwable failure, ScheduledTaskSnapshot failedSnapshot) {
        Objects.requireNonNull(failure, "failure 不能为空");
        synchronized (completionLock) {
            if (super.isDone()) {
                return false;
            }
            super.setException(failure);
            snapshot = requireOutcome(failedSnapshot, TaskOutcome.FAILED);
            return true;
        }
    }

    boolean cancel(CancellationReason reason) {
        Objects.requireNonNull(reason, "reason 不能为空");
        synchronized (completionLock) {
            if (!super.cancel(reason.interruptRequested())) {
                return false;
            }
            snapshot = snapshot.toBuilder()
                    .outcome(TaskOutcome.CANCELLED)
                    .cancellationReason(reason)
                    .build();
            return true;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return cancel(mayInterruptIfRunning
                ? CancellationReason.FUTURE_CANCELLED_INTERRUPT
                : CancellationReason.FUTURE_CANCELLED);
    }

    @Override
    public void run() {
        synchronized (completionLock) {
            if (super.isDone()) {
                return;
            }
            snapshot = snapshot.toBuilder()
                    .started(true)
                    .outcome(TaskOutcome.RUNNING)
                    .build();
        }
        super.run();
    }

    @Override
    protected void done() {
        synchronized (completionLock) {
            if (!snapshot.outcome().isTerminal()) {
                if (super.isCancelled()) {
                    snapshot = snapshot.toBuilder()
                            .outcome(TaskOutcome.CANCELLED)
                            .cancellationReason(CancellationReason.FUTURE_CANCELLED)
                            .build();
                } else {
                    try {
                        super.get();
                        snapshot = snapshot.toBuilder()
                                .executions(snapshot.executions() + 1)
                                .started(true)
                                .outcome(TaskOutcome.SUCCEEDED)
                                .build();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        snapshot = snapshot.toBuilder()
                                .executions(snapshot.executions() + 1)
                                .started(true)
                                .outcome(TaskOutcome.FAILED)
                                .lastFailure(interrupted)
                                .build();
                    } catch (ExecutionException failure) {
                        snapshot = snapshot.toBuilder()
                                .executions(snapshot.executions() + 1)
                                .started(true)
                                .outcome(TaskOutcome.FAILED)
                                .lastFailure(failure.getCause())
                                .build();
                    }
                }
            }
        }
        runTerminalAction();
    }

    @Override
    public boolean isCancelled() {
        synchronized (completionLock) {
            return super.isCancelled();
        }
    }

    @Override
    public boolean isDone() {
        synchronized (completionLock) {
            return super.isDone();
        }
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        rejectBlockingFromOwnerLoop();
        return super.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        Objects.requireNonNull(unit, "unit 不能为空");
        rejectBlockingFromOwnerLoop();
        return super.get(timeout, unit);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        Objects.requireNonNull(unit, "unit 不能为空");
        long trigger;
        synchronized (completionLock) {
            trigger = snapshot.triggerNanos();
        }
        long remaining = trigger - clock.nanoTime();
        return unit.convert(Math.max(0, remaining), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other == this) {
            return 0;
        }
        if (other instanceof EventLoopScheduledFuture<?> scheduled) {
            return ScheduledTaskSnapshotOrder.compare(snapshot(), scheduled.snapshot(), clock.nanoTime());
        }
        return Long.compare(getDelay(TimeUnit.NANOSECONDS),
                other.getDelay(TimeUnit.NANOSECONDS));
    }

    private void rejectBlockingFromOwnerLoop() {
        synchronized (completionLock) {
            if (!super.isDone() && inEventLoop.getAsBoolean()) {
                throw new BlockingOperationException(
                        "不能在所属 EventLoop 线程等待未完成 Future");
            }
        }
    }

    private void runTerminalAction() {
        Runnable action = terminalAction.get();
        if (action != null && terminalActionRun.compareAndSet(false, true)) {
            action.run();
        }
    }

    private static ScheduledTaskSnapshot requireOutcome(
            ScheduledTaskSnapshot snapshot,
            TaskOutcome expected) {
        Objects.requireNonNull(snapshot, "snapshot 不能为空");
        if (snapshot.outcome() != expected) {
            throw new IllegalArgumentException("snapshot outcome 必须为 " + expected);
        }
        return snapshot;
    }
}
