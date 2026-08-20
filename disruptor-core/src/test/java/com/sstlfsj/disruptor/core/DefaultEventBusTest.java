package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.dsl.Disruptor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DefaultEventBus 单元测试：按事件类型委托到对应管道；未知类型 fail-fast 且错误信息含类型名与用法提示。
 */
class DefaultEventBusTest {

    static class Known {
    }

    static class Unknown {
    }

    @Test
    void publisherReturnsRegisteredPipeline() {
        Pipelines pipelines = new Pipelines();
        DisruptorPipeline<Known> p = pipe("known", Known.class);
        pipelines.register(p);
        DefaultEventBus bus = new DefaultEventBus(pipelines);
        assertSame(p, bus.publisher(Known.class), "publisher 应返回注册的同一条管道");
    }

    @Test
    void publisherUnknownTypeThrowsWithHint() {
        DefaultEventBus bus = new DefaultEventBus(new Pipelines());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> bus.publisher(Unknown.class));
        assertTrue(ex.getMessage().contains(Unknown.class.getName()), "错误信息应含事件类型名");
        assertTrue(ex.getMessage().contains("@DisruptorStage"), "错误信息应给出声明用法提示");
    }

    @Test
    void remainingCapacityUnknownTypeThrows() {
        DefaultEventBus bus = new DefaultEventBus(new Pipelines());
        assertThrows(IllegalArgumentException.class, () -> bus.remainingCapacity(Unknown.class));
    }

    @Test
    void remainingCapacityDelegatesToPipeline() {
        Pipelines pipelines = new Pipelines();
        pipelines.register(pipe("known", Known.class));
        DefaultEventBus bus = new DefaultEventBus(pipelines);
        assertEquals(8, bus.remainingCapacity(Known.class), "空管道剩余容量应为 buffer 大小");
    }

    /** 构造一条最小管道（buffer=8）：真实 Disruptor 但不 start。 */
    private static <E> DisruptorPipeline<E> pipe(String name, Class<E> type) {
        Disruptor<Object> disruptor = new Disruptor<>(Object::new, 8, Thread::new);
        return new DisruptorPipeline<>(name, type, disruptor);
    }
}
