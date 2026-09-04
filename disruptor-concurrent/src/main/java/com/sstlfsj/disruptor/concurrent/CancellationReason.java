package com.sstlfsj.disruptor.concurrent;

import java.util.Objects;

/** 可扩展且可携带中断意图的取消原因。 */
public record CancellationReason(String code, boolean interruptRequested) {

    public static final CancellationReason FUTURE_CANCELLED =
            new CancellationReason("future-cancelled", false);
    public static final CancellationReason FUTURE_CANCELLED_INTERRUPT =
            new CancellationReason("future-cancelled", true);
    public static final CancellationReason EXPIRED = new CancellationReason("expired", false);
    public static final CancellationReason MAX_EXECUTIONS =
            new CancellationReason("max-executions", false);
    public static final CancellationReason SHUTDOWN = new CancellationReason("shutdown", false);
    public static final CancellationReason SHUTDOWN_NOW =
            new CancellationReason("shutdown-now", true);

    public CancellationReason {
        Objects.requireNonNull(code, "code 不能为空");
        if (code.isBlank()) {
            throw new IllegalArgumentException("code 不能为空白");
        }
    }

    public static CancellationReason of(String code) {
        return new CancellationReason(code, false);
    }

    public static CancellationReason interrupting(String code) {
        return new CancellationReason(code, true);
    }
}
