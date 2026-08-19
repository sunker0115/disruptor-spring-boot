package com.sstlfsj.disruptor.event;

import java.util.function.Consumer;

/**
 * Registry that routes published events to subscribed consumers based on the
 * runtime type of each event. Multiple consumers may subscribe for the same
 * type; all of them receive every matching event.
 */
public interface ConsumerRegistry {

    /**
     * Subscribes a consumer for events of the given type. Events are routed to
     * the consumer only when their runtime type exactly matches {@code type}.
     *
     * @param type     the event type to subscribe to
     * @param consumer the consumer invoked for each matching event
     * @param <T>      the event type
     */
    <T> void subscribe(Class<T> type, Consumer<T> consumer);

    /**
     * Dispatches an event payload to every consumer subscribed for the given
     * runtime type. No-op when the type is {@code null} or has no subscribers.
     *
     * @param type    the runtime type of the event
     * @param payload the event payload
     */
    void dispatch(Class<?> type, Object payload);
}
