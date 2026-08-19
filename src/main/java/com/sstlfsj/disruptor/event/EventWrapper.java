package com.sstlfsj.disruptor.event;

/**
 * Reusable event payload carrier stored in the ring buffer slots. The slot is
 * populated by the publisher and drained by the single event handler, which
 * extracts the payload and its runtime type before the slot is reused.
 */
public class EventWrapper {

    private Object payload;
    private Class<?> type;

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    /**
     * Clears the slot after the payload has been dispatched so the ring buffer
     * does not retain a reference to a large payload until the slot is reused.
     */
    public void clear() {
        this.payload = null;
        this.type = null;
    }
}
