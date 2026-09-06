package com.sstlfsj.disruptor.concurrent.internal;

import lombok.Builder;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** tracked/scheduled 任务的独立物理所有权记录。 */
final class AcceptedTask<V> {

    enum PhysicalState {
        WAITING,
        RUNNING,
        RETURNED,
        DISCARDED,
        CANCELLED_WAITING,
        TERMINAL
    }

    private final long acceptedSequence;
    private final Runnable shutdownNowReturnValue;
    private final EventLoopFutureTask<V> future;
    private final ScheduledTask<V> scheduledTask;
    private final AtomicReference<PhysicalState> state =
            new AtomicReference<>(PhysicalState.WAITING);
    private final AtomicBoolean cancellationQueued = new AtomicBoolean();

    @Builder
    private AcceptedTask(
            long acceptedSequence,
            Runnable shutdownNowReturnValue,
            EventLoopFutureTask<V> future,
            ScheduledTask<V> scheduledTask) {
        if (acceptedSequence < 0) {
            throw new IllegalArgumentException("acceptedSequence 不能为负数");
        }
        this.acceptedSequence = acceptedSequence;
        this.shutdownNowReturnValue = Objects.requireNonNull(
                shutdownNowReturnValue, "shutdownNowReturnValue 不能为空");
        this.future = Objects.requireNonNull(future, "future 不能为空");
        this.scheduledTask = scheduledTask;
    }

    long acceptedSequence() {
        return acceptedSequence;
    }

    EventLoopFutureTask<V> future() {
        return future;
    }

    ScheduledTask<V> scheduledTask() {
        return scheduledTask;
    }

    boolean scheduled() {
        return scheduledTask != null;
    }

    Runnable shutdownNowReturnValue() {
        return shutdownNowReturnValue;
    }

    boolean tryStart() {
        return state.compareAndSet(PhysicalState.WAITING, PhysicalState.RUNNING);
    }

    boolean tryReturn() {
        return state.compareAndSet(PhysicalState.WAITING, PhysicalState.RETURNED);
    }

    boolean tryDiscard() {
        while (true) {
            PhysicalState current = state.get();
            if (current != PhysicalState.WAITING
                    && current != PhysicalState.CANCELLED_WAITING) {
                return false;
            }
            if (state.compareAndSet(current, PhysicalState.DISCARDED)) {
                return true;
            }
        }
    }

    boolean markCancelledWaiting() {
        return state.compareAndSet(
                PhysicalState.WAITING, PhysicalState.CANCELLED_WAITING);
    }

    boolean returnToWaiting(RearmCheck check) {
        Objects.requireNonNull(check, "check 不能为空");
        return state.get() == PhysicalState.RUNNING
                && check.canRearm()
                && state.compareAndSet(PhysicalState.RUNNING, PhysicalState.WAITING);
    }

    boolean terminate() {
        return state.getAndSet(PhysicalState.TERMINAL) != PhysicalState.TERMINAL;
    }

    PhysicalState state() {
        return state.get();
    }

    boolean markCancellationQueued() {
        return cancellationQueued.compareAndSet(false, true);
    }

    void clearCancellationQueued() {
        cancellationQueued.set(false);
    }
}

@FunctionalInterface
interface RearmCheck {
    boolean canRearm();
}
