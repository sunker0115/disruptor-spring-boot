package com.sstlfsj.disruptor.event;

/**
 * Publishes events onto the LMAX Disruptor ring buffer for asynchronous
 * consumption by subscribed consumers.
 */
public interface EventPublisher {

    /**
     * Publishes an event onto the ring buffer. The event is dispatched
     * asynchronously to all consumers subscribed for its runtime type.
     *
     * @param event the event payload to publish
     */
    void publish(Object event);

    /**
     * 尝试非阻塞发布：ring buffer 有空槽时写入并返回 {@code true}；已满时立即返回
     * {@code false}，不阻塞发布线程。适合调用方在积压时自行降级（丢弃、落库、告警）。
     *
     * @param event 事件载荷
     * @return 发布成功返回 true；ring buffer 已满返回 false
     */
    boolean tryPublish(Object event);

    /**
     * @return ring buffer 当前剩余可写槽位数（堆积量 = bufferSize - 本值），供接入监控。
     */
    long remainingCapacity();
}
