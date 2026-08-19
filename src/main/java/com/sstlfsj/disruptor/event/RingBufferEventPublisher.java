package com.sstlfsj.disruptor.event;

import com.lmax.disruptor.RingBuffer;

/**
 * {@link EventPublisher} that writes events into the ring buffer slots without
 * blocking on consumer processing. Publication is asynchronous: the consumer
 * threads drain the ring buffer in the background.
 *
 * <p>This publisher only owns the ring buffer; the disruptor lifecycle (start
 * and shutdown) is managed separately by {@code DisruptorLifecycle}.</p>
 */
public class RingBufferEventPublisher implements EventPublisher {

    private final RingBuffer<EventWrapper> ringBuffer;

    public RingBufferEventPublisher(RingBuffer<EventWrapper> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void publish(Object event) {
        long sequence = ringBuffer.next();
        try {
            EventWrapper wrapper = ringBuffer.get(sequence);
            wrapper.setPayload(event);
            wrapper.setType(event == null ? null : event.getClass());
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
