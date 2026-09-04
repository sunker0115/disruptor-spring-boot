package com.sstlfsj.disruptor.concurrent;

/** EventLoop 对 accepted-but-not-cleaned 任务的容量边界。 */
public enum CapacityMode {
    BOUNDED,
    UNBOUNDED
}
