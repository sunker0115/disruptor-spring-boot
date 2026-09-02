package com.sstlfsj.disruptor.core;

/**
 * 管道健康状态。
 */
public enum PipelineHealth {
    STARTING,
    HEALTHY,
    UNHEALTHY,
    OUT_OF_SERVICE,
    TERMINATED
}
