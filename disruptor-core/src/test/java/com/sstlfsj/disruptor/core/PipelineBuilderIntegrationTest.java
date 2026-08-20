package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PipelineBuilder + DisruptorPipeline 的运行时集成测试：不依赖 Spring，直接用真实 Disruptor 跑通
 * DAG 顺序、并行分片每事件恰一次、Resettable 叶子后 cleanup，以及无参构造缺失 fail-fast——
 * 证明 core 可脱离 Spring 独立工作。
 */
class PipelineBuilderIntegrationTest {

    private static DisruptorConfig config() {
        return new DisruptorConfig(1024, DisruptorConfig.WaitStrategyType.BLOCKING, Duration.ofSeconds(2));
    }

    static class Ev {
        int id;
    }

    @Test
    void linearPipelineProcessesInDagOrder() throws Exception {
        List<String> trace = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);
        EventPipeline<Ev> def = EventPipeline.builder("linear", Ev.class)
                .stage("validate", e -> {
                    trace.add("validate:" + e.id);
                    latch.countDown();
                })
                .stage("persist", e -> {
                    trace.add("persist:" + e.id);
                    latch.countDown();
                }).after("validate")
                .build();

        DisruptorPipeline<Ev> pipeline = new PipelineBuilder(config()).build(def);
        pipeline.disruptor().start();
        try {
            pipeline.publish(e -> e.id = 7);
            assertTrue(latch.await(3, TimeUnit.SECONDS), "两阶段都应处理");
            assertEquals(List.of("validate:7", "persist:7"), trace, "应按 validate -> persist 顺序，且填充值可见");
        } finally {
            pipeline.disruptor().shutdown(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void parallelStageProcessesEachEventExactlyOnce() throws Exception {
        int n = 200;
        ConcurrentHashMap<Integer, Integer> counts = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(n);
        EventPipeline<Ev> def = EventPipeline.builder("parallel", Ev.class)
                .stage("work", e -> {
                    counts.merge(e.id, 1, Integer::sum);
                    latch.countDown();
                })
                .parallelism("work", 4)
                .build();

        DisruptorPipeline<Ev> pipeline = new PipelineBuilder(config()).build(def);
        pipeline.disruptor().start();
        try {
            for (int i = 0; i < n; i++) {
                int id = i;
                pipeline.publish(e -> e.id = id);
            }
            assertTrue(latch.await(5, TimeUnit.SECONDS), "所有事件都应处理");
            assertEquals(n, counts.size(), "每个事件都被处理");
            assertTrue(counts.values().stream().allMatch(c -> c == 1), "每个事件恰好处理一次");
        } finally {
            pipeline.disruptor().shutdown(2, TimeUnit.SECONDS);
        }
    }

    static final List<Integer> RESET_CALLS = new CopyOnWriteArrayList<>();

    public static class ResEv implements Resettable {
        int id;

        @Override
        public void reset() {
            RESET_CALLS.add(id);
        }
    }

    @Test
    void resettableEventResetAfterLeaf() throws Exception {
        RESET_CALLS.clear();
        int n = 5;
        CountDownLatch processed = new CountDownLatch(n);
        EventPipeline<ResEv> def = EventPipeline.builder("resettable", ResEv.class)
                .stage("s", e -> processed.countDown())
                .build();

        DisruptorPipeline<ResEv> pipeline = new PipelineBuilder(config()).build(def);
        pipeline.disruptor().start();
        for (int i = 0; i < n; i++) {
            int id = i;
            pipeline.publish(e -> e.id = id);
        }
        assertTrue(processed.await(3, TimeUnit.SECONDS), "所有事件都应处理");
        // reset 挂在叶子阶段之后的 cleanup handler：shutdown 排空，确保 cleanup 也已跑完
        pipeline.disruptor().shutdown(2, TimeUnit.SECONDS);
        assertEquals(n, RESET_CALLS.size(), "每个事件叶子后都应调用一次 reset");
    }

    public static class NoCtorEv {
        NoCtorEv(int x) {
        }
    }

    @Test
    void missingNoArgConstructorFailsFast() {
        EventPipeline<NoCtorEv> def = EventPipeline.builder("bad", NoCtorEv.class)
                .stage("s", e -> {
                })
                .build();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new PipelineBuilder(config()).build(def));
        assertTrue(ex.getMessage().contains("无参构造"), "应提示缺少无参构造");
    }
}
