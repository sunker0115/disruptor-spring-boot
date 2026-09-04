package com.sstlfsj.disruptor.core;

import java.time.Duration;

/** 基于单调时钟的关闭时间边界，可有界也可无界。 */
public final class ShutdownDeadline {

    private static final ShutdownDeadline UNBOUNDED = new ShutdownDeadline(null);

    private final MonotonicDeadline deadline;

    private ShutdownDeadline(MonotonicDeadline deadline) {
        this.deadline = deadline;
    }

    public static ShutdownDeadline after(Duration timeout) {
        return new ShutdownDeadline(MonotonicDeadline.after(timeout));
    }

    /**
     * 返回不随时间到期的关闭边界，供严格遵守 JDK orderly shutdown 的执行器使用。
     */
    public static ShutdownDeadline unbounded() {
        return UNBOUNDED;
    }

    public boolean isBounded() {
        return deadline != null;
    }

    public long deadlineNanos() {
        return deadline == null ? Long.MAX_VALUE : deadline.deadlineNanos();
    }

    public long remainingNanos() {
        return deadline == null ? Long.MAX_VALUE : deadline.remainingNanos();
    }

    public boolean isExpired() {
        return deadline != null && deadline.isExpired();
    }
}
