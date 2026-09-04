package com.sstlfsj.disruptor.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** 首次取消获胜、监听器可物理解注册的协作取消源。 */
public final class CancellationSource implements CancellationToken {

    private static final Logger LOGGER = LoggerFactory.getLogger(CancellationSource.class);
    private static final CancellationListenerExceptionHandler DEFAULT_EXCEPTION_HANDLER =
            (reason, failure) -> LOGGER.warn("取消监听器执行失败，reason={}", reason.code(), failure);

    private final Object lock = new Object();
    private final CancellationListenerExceptionHandler exceptionHandler;

    private CancellationReason reason;
    private ListenerNode head;
    private ListenerNode tail;
    private int listenerCount;

    public CancellationSource() {
        this(DEFAULT_EXCEPTION_HANDLER);
    }

    public CancellationSource(CancellationListenerExceptionHandler exceptionHandler) {
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler,
                "exceptionHandler 不能为空");
    }

    @Override
    public boolean isCancellationRequested() {
        synchronized (lock) {
            return reason != null;
        }
    }

    @Override
    public Optional<CancellationReason> cancellationReason() {
        synchronized (lock) {
            return Optional.ofNullable(reason);
        }
    }

    public boolean cancel(CancellationReason cancellationReason) {
        Objects.requireNonNull(cancellationReason, "cancellationReason 不能为空");
        List<ListenerNode> claimed;
        synchronized (lock) {
            if (reason != null) {
                return false;
            }
            reason = cancellationReason;
            claimed = detachAllLocked();
        }
        for (ListenerNode node : claimed) {
            invoke(node, cancellationReason);
        }
        return true;
    }

    @Override
    public CancellationRegistration onCancellation(
            Consumer<? super CancellationReason> listener) {
        return register(null, listener);
    }

    @Override
    public CancellationRegistration onCancellation(
            Executor executor,
            Consumer<? super CancellationReason> listener) {
        Objects.requireNonNull(executor, "executor 不能为空");
        return register(executor, listener);
    }

    public CancellationRegistration cancelAfter(
            CancellationReason cancellationReason,
            Duration delay,
            ScheduledExecutorService scheduler) {
        Objects.requireNonNull(cancellationReason, "cancellationReason 不能为空");
        Objects.requireNonNull(delay, "delay 不能为空");
        Objects.requireNonNull(scheduler, "scheduler 不能为空");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay 不能为负数，实际值=" + delay);
        }
        if (isCancellationRequested()) {
            return new OneShotRegistration(() -> false);
        }
        long delayNanos = toNanosSaturated(delay);
        ScheduledFuture<?> timer = scheduler.schedule(
                () -> cancel(cancellationReason), delayNanos, TimeUnit.NANOSECONDS);
        CancellationRegistration sourceRegistration = onCancellation(ignored -> timer.cancel(false));
        return new OneShotRegistration(() -> {
            boolean timerCancelled = timer.cancel(false);
            boolean listenerRemoved = sourceRegistration.unregister();
            return timerCancelled || listenerRemoved;
        });
    }

    int listenerCount() {
        synchronized (lock) {
            return listenerCount;
        }
    }

    private CancellationRegistration register(
            Executor executor,
            Consumer<? super CancellationReason> listener) {
        Objects.requireNonNull(listener, "listener 不能为空");
        ListenerNode node = new ListenerNode(executor, listener);
        CancellationReason committedReason;
        synchronized (lock) {
            committedReason = reason;
            if (committedReason == null) {
                linkLastLocked(node);
                return node;
            }
            node.state = ListenerState.CLAIMED;
        }
        invoke(node, committedReason);
        return node;
    }

    private List<ListenerNode> detachAllLocked() {
        if (head == null) {
            return List.of();
        }
        List<ListenerNode> claimed = new ArrayList<>(listenerCount);
        ListenerNode node = head;
        head = null;
        tail = null;
        listenerCount = 0;
        while (node != null) {
            ListenerNode next = node.next;
            node.previous = null;
            node.next = null;
            node.state = ListenerState.CLAIMED;
            claimed.add(node);
            node = next;
        }
        return claimed;
    }

    private void linkLastLocked(ListenerNode node) {
        node.previous = tail;
        if (tail == null) {
            head = node;
        } else {
            tail.next = node;
        }
        tail = node;
        listenerCount++;
    }

    private void unlinkLocked(ListenerNode node) {
        if (node.previous == null) {
            head = node.next;
        } else {
            node.previous.next = node.next;
        }
        if (node.next == null) {
            tail = node.previous;
        } else {
            node.next.previous = node.previous;
        }
        node.previous = null;
        node.next = null;
        listenerCount--;
    }

    private void invoke(ListenerNode node, CancellationReason cancellationReason) {
        Runnable invocation = () -> {
            try {
                node.listener.accept(cancellationReason);
            } catch (Throwable failure) {
                report(cancellationReason, failure);
            } finally {
                node.state = ListenerState.INVOKED;
            }
        };
        if (node.executor == null) {
            invocation.run();
            return;
        }
        try {
            node.executor.execute(invocation);
        } catch (Throwable failure) {
            node.state = ListenerState.INVOKED;
            report(cancellationReason, failure);
        }
    }

    private void report(CancellationReason cancellationReason, Throwable failure) {
        try {
            exceptionHandler.handle(cancellationReason, failure);
        } catch (Throwable handlerFailure) {
            LOGGER.warn("取消监听器异常处理器再次失败，reason={}",
                    cancellationReason.code(), handlerFailure);
        }
    }

    private static long toNanosSaturated(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private enum ListenerState {
        REGISTERED,
        CLAIMED,
        INVOKED,
        REMOVED
    }

    private final class ListenerNode implements CancellationRegistration {
        private final Executor executor;
        private final Consumer<? super CancellationReason> listener;
        private volatile ListenerState state = ListenerState.REGISTERED;
        private ListenerNode previous;
        private ListenerNode next;

        private ListenerNode(Executor executor, Consumer<? super CancellationReason> listener) {
            this.executor = executor;
            this.listener = listener;
        }

        @Override
        public boolean unregister() {
            synchronized (lock) {
                if (state != ListenerState.REGISTERED) {
                    return false;
                }
                unlinkLocked(this);
                state = ListenerState.REMOVED;
                return true;
            }
        }
    }

    private static final class OneShotRegistration implements CancellationRegistration {
        private final AtomicBoolean open = new AtomicBoolean(true);
        private final UnregisterAction action;

        private OneShotRegistration(UnregisterAction action) {
            this.action = action;
        }

        @Override
        public boolean unregister() {
            return open.compareAndSet(true, false) && action.unregister();
        }
    }

    @FunctionalInterface
    private interface UnregisterAction {
        boolean unregister();
    }
}
