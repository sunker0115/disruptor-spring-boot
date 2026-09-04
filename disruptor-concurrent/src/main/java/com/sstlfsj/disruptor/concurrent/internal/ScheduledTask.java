package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.CancellationRegistration;
import com.sstlfsj.disruptor.concurrent.NanoClock;
import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSpec;
import com.sstlfsj.disruptor.concurrent.TaskOutcome;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;

/** 只由 EventLoop worker 推进触发时间和周期状态的调度任务。 */
final class ScheduledTask<V> {

    private final ScheduledTaskSpec<V> spec;
    private final NanoClock clock;
    private final Consumer<Throwable> continuedFailureHandler;
    private final EventLoopFutureTask<V> future;

    private long triggerNanos;
    private int heapIndex = -1;

    ScheduledTask(
            long acceptedSequence,
            ScheduledTaskSpec<V> spec,
            long acceptedAtNanos,
            NanoClock clock) {
        this(acceptedSequence, spec, acceptedAtNanos, clock, ignored -> {
        });
    }

    ScheduledTask(
            long acceptedSequence,
            ScheduledTaskSpec<V> spec,
            long acceptedAtNanos,
            NanoClock clock,
            Consumer<Throwable> continuedFailureHandler) {
        if (acceptedSequence < 0) {
            throw new IllegalArgumentException("acceptedSequence 不能为负数");
        }
        this.spec = Objects.requireNonNull(spec, "spec 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.continuedFailureHandler = Objects.requireNonNull(
                continuedFailureHandler, "continuedFailureHandler 不能为空");
        triggerNanos = acceptedAtNanos + toNanosSaturated(spec.triggerAfter());
        OptionalLong expiresAt = spec.expiresAfter() == null
                ? OptionalLong.empty()
                : OptionalLong.of(acceptedAtNanos + toNanosSaturated(spec.expiresAfter()));
        OptionalInt maxExecutions = spec.maxExecutions() == null
                ? OptionalInt.empty()
                : OptionalInt.of(spec.maxExecutions());
        ScheduledTaskSnapshot initialSnapshot = ScheduledTaskSnapshot.builder()
                .acceptedSequence(acceptedSequence)
                .scheduleMode(spec.scheduleMode())
                .triggerNanos(triggerNanos)
                .expiresAtNanos(expiresAt)
                .priority(spec.priority())
                .executions(0)
                .maxExecutions(maxExecutions)
                .started(false)
                .outcome(TaskOutcome.WAITING)
                .build();
        future = new EventLoopFutureTask<>(clock, initialSnapshot);
        CancellationRegistration tokenRegistration = spec.cancellationToken()
                .onCancellation(future::cancel);
        future.setTerminalAction(tokenRegistration::unregister);
    }

    EventLoopFutureTask<V> future() {
        return future;
    }

    long triggerNanos() {
        return triggerNanos;
    }

    int heapIndex() {
        return heapIndex;
    }

    void heapIndex(int heapIndex) {
        this.heapIndex = heapIndex;
    }

    boolean isDue(long nowNanos) {
        return nowNanos - triggerNanos >= 0;
    }

    boolean runInvocation() {
        if (future.isDone()) {
            return false;
        }
        long startNanos = clock.nanoTime();
        ScheduledTaskSnapshot before = future.snapshot();
        if (isExpired(before, startNanos)) {
            future.cancel(CancellationReason.EXPIRED);
            return false;
        }
        future.updateNonTerminal(before.toBuilder()
                .started(true)
                .outcome(TaskOutcome.RUNNING)
                .build());

        V value = null;
        Throwable invocationFailure = null;
        try {
            spec.cancellationToken().throwIfCancellationRequested();
            value = spec.task().call(spec.context());
        } catch (Throwable failure) {
            invocationFailure = failure;
        }

        int executions = before.executions() + 1;
        ScheduledTaskSnapshot lastRun = before.toBuilder()
                .started(true)
                .executions(executions)
                .outcome(TaskOutcome.WAITING)
                .lastFailure(invocationFailure)
                .build();
        if (future.isDone()) {
            return false;
        }
        if (spec.scheduleMode() == ScheduleMode.ONE_SHOT) {
            if (invocationFailure == null) {
                future.completeSuccess(value, lastRun.toBuilder()
                        .outcome(TaskOutcome.SUCCEEDED)
                        .build());
            } else {
                future.completeFailure(invocationFailure, lastRun.toBuilder()
                        .outcome(TaskOutcome.FAILED)
                        .build());
            }
            return false;
        }
        if (invocationFailure != null && !spec.continueOnFailure()) {
            future.completeFailure(invocationFailure, lastRun.toBuilder()
                    .outcome(TaskOutcome.FAILED)
                    .build());
            return false;
        }

        future.updateNonTerminal(lastRun);
        if (invocationFailure != null) {
            continuedFailureHandler.accept(invocationFailure);
        }
        if (spec.maxExecutions() != null && executions >= spec.maxExecutions()) {
            future.cancel(CancellationReason.MAX_EXECUTIONS);
            return false;
        }
        long completedAtNanos = clock.nanoTime();
        if (isExpired(lastRun, completedAtNanos)) {
            future.cancel(CancellationReason.EXPIRED);
            return false;
        }

        try {
            triggerNanos = nextTrigger(lastRun, completedAtNanos);
        } catch (Throwable failure) {
            future.completeFailure(failure, lastRun.toBuilder()
                    .outcome(TaskOutcome.FAILED)
                    .lastFailure(failure)
                    .build());
            return false;
        }
        future.updateNonTerminal(lastRun.toBuilder()
                .triggerNanos(triggerNanos)
                .build());
        return true;
    }

    int compareTo(ScheduledTask<?> other) {
        return ScheduledTaskSnapshotOrder.compare(
                future.snapshot(), other.future.snapshot(), clock.nanoTime());
    }

    private long nextTrigger(ScheduledTaskSnapshot lastRun, long completedAtNanos) throws Exception {
        return switch (spec.scheduleMode()) {
            case FIXED_RATE -> triggerNanos + toNanosSaturated(spec.period());
            case FIXED_DELAY -> completedAtNanos + toNanosSaturated(spec.period());
            case DYNAMIC_DELAY -> {
                Duration delay = Objects.requireNonNull(spec.dynamicDelay().nextDelay(lastRun),
                        "dynamicDelay 不能返回 null");
                if (delay.isNegative()) {
                    throw new IllegalArgumentException("dynamicDelay 不能返回负数，实际值=" + delay);
                }
                yield completedAtNanos + toNanosSaturated(delay);
            }
            case ONE_SHOT -> throw new IllegalStateException("ONE_SHOT 不计算下一次 trigger");
        };
    }

    private static boolean isExpired(ScheduledTaskSnapshot snapshot, long nowNanos) {
        return snapshot.expiresAtNanos().isPresent()
                && nowNanos - snapshot.expiresAtNanos().getAsLong() >= 0;
    }

    private static long toNanosSaturated(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }
}
