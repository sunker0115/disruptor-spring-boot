package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.dsl.Disruptor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pipelines 容器单元测试：按事件类型注册/查找、重复事件类型 fail-fast、未知类型返回 null、all() 反映已注册。
 */
class PipelinesTest {

    static class EvA {
    }

    static class EvB {
    }

    @Test
    void registerThenGetReturnsSameInstance() {
        Pipelines pipelines = new Pipelines();
        DisruptorPipeline<EvA> p = pipe("a", EvA.class);
        pipelines.register(p);
        assertSame(p, pipelines.get(EvA.class));
    }

    @Test
    void getUnknownTypeReturnsNull() {
        assertNull(new Pipelines().get(EvA.class));
    }

    @Test
    void duplicateEventTypeFailsFast() {
        Pipelines pipelines = new Pipelines();
        pipelines.register(pipe("a1", EvA.class));
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> pipelines.register(pipe("a2", EvA.class)));
        assertTrue(ex.getMessage().contains(EvA.class.getName()));
    }

    @Test
    void allReflectsRegisteredPipelines() {
        Pipelines pipelines = new Pipelines();
        pipelines.register(pipe("a", EvA.class));
        pipelines.register(pipe("b", EvB.class));
        assertEquals(2, pipelines.all().size());
    }

    /** 构造一条仅用于注册/查找的最小管道：真实 Disruptor 但不 start（不产生线程）。 */
    private static <E> DisruptorPipeline<E> pipe(String name, Class<E> type) {
        Disruptor<Object> disruptor = new Disruptor<>(Object::new, 8, Thread::new);
        return new DisruptorPipeline<>(name, type, disruptor);
    }
}
