package com.sstlfsj.disruptor.core;

/**
 * 管道生命周期阶段。
 */
public enum PipelineLifecycle {
    NEW,
    STARTING,
    RUNNING,
    QUIESCING,
    STOPPING,
    TERMINATED
}
