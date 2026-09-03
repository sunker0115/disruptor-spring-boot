package com.sstlfsj.disruptor.core;

import java.time.Duration;
import java.util.Objects;

/** 基于单调时钟的一次性绝对关闭截止时间。 */
public final class ShutdownDeadline {

    private final long deadlineNanos;

    private ShutdownDeadline(long deadlineNanos) {
        this.deadlineNanos = deadlineNanos;
    }

    public static ShutdownDeadline after(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout 不能为空");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout 不能为负数，实际值=" + timeout);
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long now = System.nanoTime();
        long deadline;
        try {
            deadline = Math.addExact(now, timeoutNanos);
        } catch (ArithmeticException overflow) {
            deadline = Long.MAX_VALUE;
        }
        return new ShutdownDeadline(deadline);
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    public long remainingNanos() {
        long remaining = deadlineNanos - System.nanoTime();
        return remaining > 0L ? remaining : 0L;
    }

    public boolean isExpired() {
        return deadlineNanos - System.nanoTime() <= 0L;
    }
}
