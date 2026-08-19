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
}
