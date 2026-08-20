package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EventPipeline.Builder 纯逻辑单元测试：阶段构建、after/parallelism 记录、重名与非法调用 fail-fast、
 * 产出列表不可变。
 */
class EventPipelineBuilderTest {

    static class Ev {
    }

    private static final Consumer<Ev> NOOP = e -> {
    };

    @Test
    void buildsStagesWithAfterAndParallelism() {
        EventPipeline<Ev> p = EventPipeline.builder("p", Ev.class)
                .stage("a", NOOP)
                .stage("b", NOOP).after("a")
                .parallelism("b", 4)
                .build();

        assertEquals("p", p.pipeline());
        assertEquals(Ev.class, p.eventType());
        assertEquals(2, p.stages().size());

        EventPipeline.Stage<Ev> a = p.stages().get(0);
        assertEquals(List.of(), a.after());
        assertEquals(1, a.parallelism(), "未设置的阶段默认 parallelism=1");

        EventPipeline.Stage<Ev> b = p.stages().get(1);
        assertEquals("b", b.name());
        assertEquals(List.of("a"), b.after());
        assertEquals(4, b.parallelism());
    }

    @Test
    void duplicateStageNameFailsFast() {
        EventPipeline.Builder<Ev> b = EventPipeline.builder("p", Ev.class).stage("a", NOOP);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> b.stage("a", NOOP));
        assertTrue(ex.getMessage().contains("a"));
    }

    @Test
    void afterWithoutPrecedingStageFailsFast() {
        EventPipeline.Builder<Ev> b = EventPipeline.builder("p", Ev.class);
        assertThrows(IllegalStateException.class, () -> b.after("x"));
    }

    @Test
    void parallelismOnUnknownStageFailsFast() {
        EventPipeline.Builder<Ev> b = EventPipeline.builder("p", Ev.class).stage("a", NOOP);
        assertThrows(IllegalStateException.class, () -> b.parallelism("nope", 2));
    }

    @Test
    void builtStageListsAreImmutable() {
        EventPipeline<Ev> p = EventPipeline.builder("p", Ev.class).stage("a", NOOP).build();
        assertThrows(UnsupportedOperationException.class, () -> p.stages().add(null));
        assertThrows(UnsupportedOperationException.class, () -> p.stages().get(0).after().add("x"));
    }
}
