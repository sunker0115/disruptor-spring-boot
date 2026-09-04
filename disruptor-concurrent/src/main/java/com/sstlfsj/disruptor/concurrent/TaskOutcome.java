package com.sstlfsj.disruptor.concurrent;

/** Future 对外可见的执行结果，与内部物理清理状态分离。 */
public enum TaskOutcome {
    WAITING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
