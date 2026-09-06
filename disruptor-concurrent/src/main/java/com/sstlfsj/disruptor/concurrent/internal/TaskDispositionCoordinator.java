package com.sstlfsj.disruptor.concurrent.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/** 未开始任务处置的所有权与完成阶段；生命周期和失败首因仍由 supervisor 管理。 */
final class TaskDispositionCoordinator {

    enum State { NONE, RETURNING, RETURNED, DISCARDING, DISCARDED }

    private final AtomicReference<State> state = new AtomicReference<>(State.NONE);
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    boolean tryBeginReturning() {
        return state.compareAndSet(State.NONE, State.RETURNING);
    }

    boolean tryBeginDiscarding() {
        return state.compareAndSet(State.NONE, State.DISCARDING);
    }

    void failReturningToDiscarding(Throwable cause) {
        Objects.requireNonNull(cause, "cause 不能为空");
        // RETURNING 只有调用方能完成；先交付失败，接管者才可完成 DISCARD。
        if (state.get() == State.RETURNING) {
            completion.completeExceptionally(cause);
            state.compareAndSet(State.RETURNING, State.DISCARDING);
        }
    }

    void completeReturned() {
        if (state.compareAndSet(State.RETURNING, State.RETURNED)) {
            completion.complete(null);
        }
    }

    void completeDiscarded() {
        if (state.compareAndSet(State.DISCARDING, State.DISCARDED)) {
            completion.complete(null);
        }
    }

    State state() {
        return state.get();
    }

    boolean isTerminal() {
        State current = state.get();
        return current == State.RETURNED || current == State.DISCARDED;
    }

    /** 失败结果不等于物理处置终态，失败后仍须接管到 DISCARDED。 */
    CompletionStage<Void> completion() {
        return completion.minimalCompletionStage();
    }
}
