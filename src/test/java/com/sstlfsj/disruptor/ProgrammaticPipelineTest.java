package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.event.DisruptorStage;
import com.sstlfsj.disruptor.event.EventBus;
import com.sstlfsj.disruptor.event.EventPipeline;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 编程式 EventPipeline DSL 验收：菱形 e2e、并行分片、与声明式并存、管道名冲突 fail-fast。
 */
class ProgrammaticPipelineTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    public static class OrderEvent {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    // ---- 编程式菱形 validate -> (persist, audit) -> notify ----
    static final java.util.List<String> TRACE = new CopyOnWriteArrayList<>();
    static final CountDownLatch LATCH = new CountDownLatch(4);

    @Configuration
    static class DiamondConfig {
        @Bean
        EventPipeline<OrderEvent> orderPipeline() {
            return EventPipeline.builder("order", OrderEvent.class)
                    .stage("validate", e -> {
                        TRACE.add("validate:" + e.getId());
                        LATCH.countDown();
                    })
                    .stage("persist", e -> {
                        TRACE.add("persist");
                        LATCH.countDown();
                    }).after("validate")
                    .stage("audit", e -> {
                        TRACE.add("audit");
                        LATCH.countDown();
                    }).after("validate")
                    .stage("notify", e -> {
                        TRACE.add("notify");
                        LATCH.countDown();
                    }).after("persist", "audit")
                    .build();
        }
    }

    @Test
    void programmaticDiamondFlowsInOrder() {
        runner.withUserConfiguration(DiamondConfig.class).run(ctx -> {
            ctx.getBean(EventBus.class).publish(OrderEvent.class, e -> e.setId("A"));
            assertTrue(LATCH.await(3, TimeUnit.SECONDS), "四阶段都应处理");
            assertEquals("validate:A", TRACE.get(0), "validate 最先且填充可见");
            assertEquals("notify", TRACE.get(3), "notify 汇聚在 persist、audit 之后");
        });
    }

    // ---- 编程式并行分片 ----
    public static class WorkEvent {
        private int n;

        public void setN(int n) {
            this.n = n;
        }
    }

    static final AtomicInteger COUNT = new AtomicInteger();
    static final CountDownLatch WORK_LATCH = new CountDownLatch(40);

    @Configuration
    static class ParallelConfig {
        @Bean
        EventPipeline<WorkEvent> workPipeline() {
            return EventPipeline.builder("work", WorkEvent.class)
                    .stage("process", e -> {
                        COUNT.incrementAndGet();
                        WORK_LATCH.countDown();
                    })
                    .parallelism("process", 4)
                    .build();
        }
    }

    @Test
    void programmaticParallelProcessesEachOnce() {
        runner.withUserConfiguration(ParallelConfig.class).run(ctx -> {
            EventBus bus = ctx.getBean(EventBus.class);
            for (int i = 0; i < 40; i++) {
                int v = i;
                bus.publish(WorkEvent.class, e -> e.setN(v));
            }
            assertTrue(WORK_LATCH.await(3, TimeUnit.SECONDS), "全部应处理");
            assertEquals(40, COUNT.get(), "4 分片下每事件恰好处理一次");
        });
    }

    // ---- 声明式与编程式并存 ----
    public static class PayEvent {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    static final CountDownLatch COEXIST_LATCH = new CountDownLatch(2);

    @Configuration
    static class CoexistConfig {
        @Bean
        EventPipeline<OrderEvent> orderPipeline() {
            return EventPipeline.builder("order", OrderEvent.class)
                    .stage("handle", e -> COEXIST_LATCH.countDown())
                    .build();
        }

        @Bean
        AnnotatedPay annotatedPay() {
            return new AnnotatedPay();
        }
    }

    static class AnnotatedPay {
        @DisruptorStage(pipeline = "pay", name = "handle")
        public void handle(PayEvent e) {
            COEXIST_LATCH.countDown();
        }
    }

    @Test
    void programmaticAndAnnotatedCoexist() {
        runner.withUserConfiguration(CoexistConfig.class).run(ctx -> {
            EventBus bus = ctx.getBean(EventBus.class);
            bus.publish(OrderEvent.class, e -> e.setId("O"));   // 编程式管道
            bus.publish(PayEvent.class, e -> e.setId("P"));     // 声明式管道
            assertTrue(COEXIST_LATCH.await(3, TimeUnit.SECONDS),
                    "编程式与声明式两条管道应各自工作");
        });
    }

    // ---- 管道名冲突 fail-fast ----
    public static class EvX {
    }

    public static class EvY {
    }

    @Configuration
    static class DupNameConfig {
        @Bean
        EventPipeline<EvX> dupProgrammatic() {
            return EventPipeline.builder("dup", EvX.class).stage("s", e -> {
            }).build();
        }

        @Bean
        DupAnnotated dupAnnotated() {
            return new DupAnnotated();
        }
    }

    static class DupAnnotated {
        @DisruptorStage(pipeline = "dup", name = "s")
        public void s(EvY e) {
        }
    }

    @Test
    void duplicatePipelineNameAcrossSourcesFailsFast() {
        runner.withUserConfiguration(DupNameConfig.class).run(ctx -> {
            Throwable t = ctx.getStartupFailure();
            boolean illegalState = false;
            for (Throwable c = t; c != null; c = c.getCause()) {
                if (c instanceof IllegalStateException) {
                    illegalState = true;
                    break;
                }
            }
            assertTrue(illegalState, "声明式与编程式同名管道应启动失败");
        });
    }
}
