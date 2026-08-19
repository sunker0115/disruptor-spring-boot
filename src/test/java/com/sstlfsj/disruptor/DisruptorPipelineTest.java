package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.event.DisruptorListener;
import com.sstlfsj.disruptor.event.EventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 阶段流水线编排验收：下游阶段在上游之后处理同一事件、菱形汇聚、未声明阶段 fail-fast。
 */
class DisruptorPipelineTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    public record Msg(String id) {
    }

    // ---- 两阶段线性顺序：default -> b ----
    static final List<String> LINEAR_TRACE = new CopyOnWriteArrayList<>();
    static final CountDownLatch LINEAR_LATCH = new CountDownLatch(2);

    @Configuration
    static class LinearConfig {
        @Bean
        LinearListeners linearListeners() {
            return new LinearListeners();
        }
    }

    static class LinearListeners {
        @DisruptorListener // 默认 default 阶段（源头）
        public void source(Msg m) {
            LINEAR_TRACE.add("default");
            LINEAR_LATCH.countDown();
        }

        @DisruptorListener(stage = "b")
        public void downstream(Msg m) {
            LINEAR_TRACE.add("b");
            LINEAR_LATCH.countDown();
        }
    }

    @Test
    void downstreamStageRunsAfterUpstream() {
        runner.withPropertyValues("disruptor.pipeline.b.after[0]=default")
                .withUserConfiguration(LinearConfig.class)
                .run(ctx -> {
                    ctx.getBean(EventPublisher.class).publish(new Msg("1"));
                    assertTrue(LINEAR_LATCH.await(3, TimeUnit.SECONDS), "两阶段都应处理");
                    assertEquals(List.of("default", "b"), LINEAR_TRACE,
                            "b 阶段应在 default 阶段之后处理同一事件");
                });
    }

    // ---- 菱形：default -> (b, c) -> d ----
    static final List<String> DIAMOND_TRACE = new CopyOnWriteArrayList<>();
    static final CountDownLatch DIAMOND_LATCH = new CountDownLatch(4);

    @Configuration
    static class DiamondConfig {
        @Bean
        DiamondListeners diamondListeners() {
            return new DiamondListeners();
        }
    }

    static class DiamondListeners {
        @DisruptorListener
        public void a(Msg m) {
            DIAMOND_TRACE.add("a");
            DIAMOND_LATCH.countDown();
        }

        @DisruptorListener(stage = "b")
        public void b(Msg m) {
            DIAMOND_TRACE.add("b");
            DIAMOND_LATCH.countDown();
        }

        @DisruptorListener(stage = "c")
        public void c(Msg m) {
            DIAMOND_TRACE.add("c");
            DIAMOND_LATCH.countDown();
        }

        @DisruptorListener(stage = "d")
        public void d(Msg m) {
            DIAMOND_TRACE.add("d");
            DIAMOND_LATCH.countDown();
        }
    }

    @Test
    void joinStageRunsAfterAllBranches() {
        runner.withPropertyValues(
                        "disruptor.pipeline.b.after[0]=default",
                        "disruptor.pipeline.c.after[0]=default",
                        "disruptor.pipeline.d.after[0]=b",
                        "disruptor.pipeline.d.after[1]=c")
                .withUserConfiguration(DiamondConfig.class)
                .run(ctx -> {
                    ctx.getBean(EventPublisher.class).publish(new Msg("1"));
                    assertTrue(DIAMOND_LATCH.await(3, TimeUnit.SECONDS), "四阶段都应处理");
                    assertEquals("a", DIAMOND_TRACE.get(0), "default(源头)阶段应最先处理");
                    assertTrue(DIAMOND_TRACE.indexOf("d") > DIAMOND_TRACE.indexOf("b"), "d 应在 b 之后");
                    assertTrue(DIAMOND_TRACE.indexOf("d") > DIAMOND_TRACE.indexOf("c"), "d 应在 c 之后");
                });
    }

    // ---- 未声明阶段 fail-fast ----
    @Configuration
    static class UndeclaredStageConfig {
        @Bean
        UndeclaredListener undeclaredListener() {
            return new UndeclaredListener();
        }
    }

    static class UndeclaredListener {
        @DisruptorListener(stage = "ghost")
        public void h(Msg m) {
        }
    }

    @Test
    void undeclaredStageFailsFast() {
        runner.withUserConfiguration(UndeclaredStageConfig.class).run(ctx ->
                assertTrue(ctx.getStartupFailure() != null
                                && hasIllegalStateInChain(ctx.getStartupFailure()),
                        "引用未声明阶段应导致启动失败"));
    }

    private static boolean hasIllegalStateInChain(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof IllegalStateException) {
                return true;
            }
        }
        return false;
    }
}
