package com.sstlfsj.disruptor.core;

/**
 * 受监督 worker 集的生命周期阶段。
 */
public enum SupervisedLifecycle {
    NEW,
    STARTING,
    RUNNING,
    QUIESCING,
    STOPPING,
    TERMINATED
}
