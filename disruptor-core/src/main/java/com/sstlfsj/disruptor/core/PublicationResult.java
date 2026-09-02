package com.sstlfsj.disruptor.core;

/**
 * 事件发布结果。
 */
public enum PublicationResult {
    PUBLISHED,
    CAPACITY_EXHAUSTED,
    TIMED_OUT,
    NOT_RUNNING,
    PIPELINE_FAILED
}
