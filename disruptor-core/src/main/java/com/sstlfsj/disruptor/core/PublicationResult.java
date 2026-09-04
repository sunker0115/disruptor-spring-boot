package com.sstlfsj.disruptor.core;

/**
 * 事件发布结果。
 */
public enum PublicationResult {
    /** 事件已发布。 */
    PUBLISHED,
    /** 本次非阻塞尝试时 RingBuffer 无可用容量。 */
    CAPACITY_EXHAUSTED,
    /** 在给定期限内未取得 RingBuffer 容量。 */
    TIMED_OUT,
    /** 管道尚未启动或已进入正常关闭。 */
    NOT_RUNNING,
    /** 管道已锁定 worker 或基础设施故障。 */
    PIPELINE_FAILED
}
