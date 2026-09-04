package com.sstlfsj.disruptor.concurrent.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** accepted 任务的 registry 记录，将 Future 终态与物理清理分离。 */
final class AcceptedTask {

    enum PhysicalState {
        WAITING,
        RUNNING,
        CANCELLED_WAITING,
        RETURNED,
        TERMINAL
    }

    private final long acceptedSequence;
    private final Runnable originalRunnable;
    private final EventLoopFutureTask<?> future;
    private final Runnable cleanup;
    private final AtomicReference<PhysicalState> state = new AtomicReference<>(PhysicalState.WAITING);
    private final AtomicBoolean cancellationQueued = new AtomicBoolean();
    private final AtomicBoolean cleaned = new AtomicBoolean();

    AcceptedTask(
            long acceptedSequence,
            Runnable originalRunnable,
            EventLoopFutureTask<?> future,
            Runnable cleanup) {
        if (acceptedSequence < 0) {
            throw new IllegalArgumentException("acceptedSequence 不能为负数");
        }
        this.acceptedSequence = acceptedSequence;
        this.originalRunnable = Objects.requireNonNull(originalRunnable,
                "originalRunnable 不能为空");
        this.future = Objects.requireNonNull(future, "future 不能为空");
        this.cleanup = Objects.requireNonNull(cleanup, "cleanup 不能为空");
    }

    long acceptedSequence() {
        return acceptedSequence;
    }

    Runnable originalRunnable() {
        return originalRunnable;
    }

    EventLoopFutureTask<?> future() {
        return future;
    }

    PhysicalState state() {
        return state.get();
    }

    boolean tryStart() {
        return state.compareAndSet(PhysicalState.WAITING, PhysicalState.RUNNING);
    }

    boolean returnToWaiting() {
        return state.compareAndSet(PhysicalState.RUNNING, PhysicalState.WAITING);
    }

    boolean markCancelledWaiting() {
        return state.compareAndSet(PhysicalState.WAITING, PhysicalState.CANCELLED_WAITING);
    }

    boolean tryReturn() {
        return state.compareAndSet(PhysicalState.WAITING, PhysicalState.RETURNED);
    }

    void terminate() {
        PhysicalState previous = state.getAndSet(PhysicalState.TERMINAL);
        if (previous != PhysicalState.TERMINAL && cleaned.compareAndSet(false, true)) {
            cleanup.run();
        }
    }

    boolean markCancellationQueued() {
        return cancellationQueued.compareAndSet(false, true);
    }

    void clearCancellationQueued() {
        cancellationQueued.set(false);
    }
}
