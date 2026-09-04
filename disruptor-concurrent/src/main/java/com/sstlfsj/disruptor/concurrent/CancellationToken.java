package com.sstlfsj.disruptor.concurrent;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/** 只读的协作取消信号。 */
public interface CancellationToken {

    CancellationToken NONE = new CancellationToken() {
        @Override
        public boolean isCancellationRequested() {
            return false;
        }

        @Override
        public Optional<CancellationReason> cancellationReason() {
            return Optional.empty();
        }

        @Override
        public CancellationRegistration onCancellation(
                Consumer<? super CancellationReason> listener) {
            java.util.Objects.requireNonNull(listener, "listener 不能为空");
            return () -> true;
        }

        @Override
        public CancellationRegistration onCancellation(
                Executor executor,
                Consumer<? super CancellationReason> listener) {
            java.util.Objects.requireNonNull(executor, "executor 不能为空");
            java.util.Objects.requireNonNull(listener, "listener 不能为空");
            return () -> true;
        }
    };

    boolean isCancellationRequested();

    Optional<CancellationReason> cancellationReason();

    CancellationRegistration onCancellation(Consumer<? super CancellationReason> listener);

    CancellationRegistration onCancellation(
            Executor executor,
            Consumer<? super CancellationReason> listener);

    default void throwIfCancellationRequested() {
        cancellationReason().ifPresent(reason -> {
            throw new CancellationException("task cancelled: " + reason.code());
        });
    }

    static CancellationToken none() {
        return NONE;
    }
}
