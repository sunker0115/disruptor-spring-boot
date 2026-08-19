package com.sstlfsj.disruptor.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Default {@link ConsumerRegistry} backed by a concurrent map of type to
 * consumer lists. Safe for concurrent subscription and dispatch.
 */
public class DefaultConsumerRegistry implements ConsumerRegistry {

    private static final Logger log = LoggerFactory.getLogger(DefaultConsumerRegistry.class);

    private final Map<Class<?>, List<Consumer<?>>> consumers = new ConcurrentHashMap<>();

    @Override
    public <T> void subscribe(Class<T> type, Consumer<T> consumer) {
        consumers.computeIfAbsent(type, key -> new CopyOnWriteArrayList<>()).add(consumer);
    }

    @Override
    public void dispatch(Class<?> type, Object payload) {
        if (type == null) {
            return;
        }
        List<Consumer<?>> list = consumers.get(type);
        if (list == null || list.isEmpty()) {
            return;
        }
        for (Consumer<?> consumer : list) {
            @SuppressWarnings("unchecked")
            Consumer<Object> typedConsumer = (Consumer<Object>) consumer;
            try {
                typedConsumer.accept(payload);
            } catch (Exception e) {
                // 隔离单个消费者的异常：不让它牵连同类型的其它消费者，也不外抛到消费线程。
                log.error("消费者处理事件异常，type={}，已跳过该消费者", type.getName(), e);
            }
        }
    }
}
