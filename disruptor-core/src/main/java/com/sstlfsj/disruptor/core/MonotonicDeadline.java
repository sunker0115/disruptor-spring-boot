package com.sstlfsj.disruptor.core;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/** 用起点与已用时长表达的回绕安全单调截止时间。 */
final class MonotonicDeadline {

    private final LongSupplier nanoTime;
    private final long startedAtNanos;
    private final long budgetNanos;

    private MonotonicDeadline(
            LongSupplier nanoTime,
            long startedAtNanos,
            long budgetNanos) {
        this.nanoTime = nanoTime;
        this.startedAtNanos = startedAtNanos;
        this.budgetNanos = budgetNanos;
    }

    static MonotonicDeadline after(Duration timeout) {
        return after(timeout, System::nanoTime);
    }

    static MonotonicDeadline after(Duration timeout, LongSupplier nanoTime) {
        Objects.requireNonNull(timeout, "timeout 不能为空");
        Objects.requireNonNull(nanoTime, "nanoTime 不能为空");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout 不能为负数，实际值=" + timeout);
        }
        long budgetNanos;
        try {
            budgetNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            budgetNanos = Long.MAX_VALUE;
        }
        return new MonotonicDeadline(nanoTime, nanoTime.getAsLong(), budgetNanos);
    }

    long deadlineNanos() {
        return startedAtNanos + budgetNanos;
    }

    long remainingNanos() {
        long elapsedNanos = nanoTime.getAsLong() - startedAtNanos;
        if (elapsedNanos < 0L || elapsedNanos >= budgetNanos) {
            return 0L;
        }
        return budgetNanos - elapsedNanos;
    }

    boolean isExpired() {
        return remainingNanos() == 0L;
    }
}
