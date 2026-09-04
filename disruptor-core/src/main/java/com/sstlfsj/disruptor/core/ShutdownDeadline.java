package com.sstlfsj.disruptor.core;

import java.time.Duration;
/** 基于单调时钟的一次性绝对关闭截止时间。 */
public final class ShutdownDeadline {

    private final MonotonicDeadline deadline;

    private ShutdownDeadline(MonotonicDeadline deadline) {
        this.deadline = deadline;
    }

    public static ShutdownDeadline after(Duration timeout) {
        return new ShutdownDeadline(MonotonicDeadline.after(timeout));
    }

    public long deadlineNanos() {
        return deadline.deadlineNanos();
    }

    public long remainingNanos() {
        return deadline.remainingNanos();
    }

    public boolean isExpired() {
        return deadline.isExpired();
    }
}
