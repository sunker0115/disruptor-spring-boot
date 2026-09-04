package com.sstlfsj.disruptor.concurrent.internal;

import java.util.concurrent.atomic.AtomicReference;

/** 一次准入的活动令牌及其 outstanding 容量所有权。 */
final class AdmissionToken implements AutoCloseable {

    private final TaskAdmissionGate gate;
    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);

    AdmissionToken(TaskAdmissionGate gate) {
        this.gate = gate;
    }

    void commit() {
        while (true) {
            State current = state.get();
            if (current == State.ACTIVE
                    && state.compareAndSet(State.ACTIVE, State.COMMITTED)) {
                gate.finishAdmission(false);
                return;
            }
            if (current == State.RELEASE_PENDING
                    && state.compareAndSet(State.RELEASE_PENDING, State.RELEASED)) {
                gate.finishAdmission(true);
                return;
            }
            if (current != State.ACTIVE && current != State.RELEASE_PENDING) {
                throw new IllegalStateException("准入令牌不能重复提交");
            }
        }
    }

    void abort() {
        if (state.compareAndSet(State.ACTIVE, State.ABORTED)) {
            gate.finishAdmission(true);
        }
    }

    void releaseOutstanding() {
        while (true) {
            State current = state.get();
            if (current == State.ACTIVE
                    && state.compareAndSet(State.ACTIVE, State.RELEASE_PENDING)) {
                return;
            }
            if (current == State.COMMITTED
                    && state.compareAndSet(State.COMMITTED, State.RELEASED)) {
                gate.releaseOutstanding();
                return;
            }
            if (current == State.RELEASE_PENDING
                    || current == State.RELEASED
                    || current == State.ABORTED) {
                return;
            }
        }
    }

    boolean active() {
        return state.get() == State.ACTIVE;
    }

    @Override
    public void close() {
        abort();
    }

    private enum State {
        ACTIVE,
        RELEASE_PENDING,
        COMMITTED,
        RELEASED,
        ABORTED
    }
}
