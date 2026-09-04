package com.sstlfsj.disruptor.concurrent.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** accepted 任务的 registry 记录，将 Future 终态与物理清理分离。 */
final class AcceptedTask<V> {

    enum PhysicalState {
        WAITING,
        RUNNING,
        CANCELLED_WAITING,
        RETURNED,
        TERMINAL
    }

    private final long acceptedSequence;
    private final Runnable originalRunnable;
    private final EventLoopFutureTask<V> future;
    private final AtomicReference<PhysicalState> state = new AtomicReference<>(PhysicalState.WAITING);
    private final AtomicBoolean cancellationQueued = new AtomicBoolean();

    AcceptedTask(
            long acceptedSequence,
            Runnable originalRunnable,
            EventLoopFutureTask<V> future) {
        if (acceptedSequence < 0) {
            throw new IllegalArgumentException("acceptedSequence 不能为负数");
        }
        this.acceptedSequence = acceptedSequence;
        this.originalRunnable = Objects.requireNonNull(originalRunnable,
                "originalRunnable 不能为空");
        this.future = Objects.requireNonNull(future, "future 不能为空");
    }

    long acceptedSequence() {
        return acceptedSequence;
    }

    Runnable originalRunnable() {
        return originalRunnable;
    }

    EventLoopFutureTask<V> future() {
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

    boolean terminate() {
        return state.getAndSet(PhysicalState.TERMINAL) != PhysicalState.TERMINAL;
    }

    boolean markCancellationQueued() {
        return cancellationQueued.compareAndSet(false, true);
    }

    void clearCancellationQueued() {
        cancellationQueued.set(false);
    }
}
