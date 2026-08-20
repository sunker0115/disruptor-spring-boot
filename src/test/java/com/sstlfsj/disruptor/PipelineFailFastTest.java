package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.event.DisruptorStage;
import com.sstlfsj.disruptor.event.EventBus;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 快速失败验收：非单参数、同管道类型不一致、事件类型跨管道重复、无无参构造、环、缺失依赖
 * 均在启动时失败；对未声明类型发布在运行时抛 IllegalArgumentException。
 */
class PipelineFailFastTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    public static class EvA {
    }

    public static class EvB {
    }

    public static class EvC {
    }

    public static class NoCtorEvent {
        public NoCtorEvent(int x) {
        }
    }

    private static boolean startupFailedWithIllegalState(org.springframework.boot.test.context.assertj.AssertableApplicationContext ctx) {
        Throwable t = ctx.getStartupFailure();
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof IllegalStateException) {
                return true;
            }
        }
        return false;
    }

    // ---- 非单参数 ----
    @Configuration
    static class BadArityConfig {
        @Bean
        BadArity badArity() {
            return new BadArity();
        }
    }

    static class BadArity {
        @DisruptorStage(pipeline = "x", name = "s")
        public void h(EvA a, String b) {
        }
    }

    @Test
    void nonSingleParamFailsFast() {
        runner.withUserConfiguration(BadArityConfig.class)
                .run(ctx -> assertTrue(startupFailedWithIllegalState(ctx), "非单参数应启动失败"));
    }

    // ---- 同管道类型不一致 ----
    @Configuration
    static class TypeMismatchConfig {
        @Bean
        TypeMismatch typeMismatch() {
            return new TypeMismatch();
        }
    }

    static class TypeMismatch {
        @DisruptorStage(pipeline = "y", name = "s1")
        public void a(EvA e) {
        }

        @DisruptorStage(pipeline = "y", name = "s2", after = "s1")
        public void b(EvB e) {
        }
    }

    @Test
    void mixedEventTypesInPipelineFailFast() {
        runner.withUserConfiguration(TypeMismatchConfig.class)
                .run(ctx -> assertTrue(startupFailedWithIllegalState(ctx), "同管道类型不一致应启动失败"));
    }

    // ---- 事件类型跨管道重复 ----
    @Configuration
    static class DupTypeConfig {
        @Bean
        DupType dupType() {
            return new DupType();
        }
    }

    static class DupType {
        @DisruptorStage(pipeline = "p1", name = "s")
        public void a(EvA e) {
        }

        @DisruptorStage(pipeline = "p2", name = "s")
        public void b(EvA e) {
        }
    }

    @Test
    void duplicateEventTypeAcrossPipelinesFailsFast() {
        runner.withUserConfiguration(DupTypeConfig.class)
                .run(ctx -> assertTrue(startupFailedWithIllegalState(ctx), "事件类型跨管道重复应启动失败"));
    }

    // ---- 无无参构造 ----
    @Configuration
    static class NoCtorConfig {
        @Bean
        NoCtorStage noCtorStage() {
            return new NoCtorStage();
        }
    }

    static class NoCtorStage {
        @DisruptorStage(pipeline = "nc", name = "s")
        public void h(NoCtorEvent e) {
        }
    }

    @Test
    void noDefaultConstructorFailsFast() {
        runner.withUserConfiguration(NoCtorConfig.class)
                .run(ctx -> assertTrue(startupFailedWithIllegalState(ctx), "事件缺无参构造应启动失败"));
    }

    // ---- 环 ----
    @Configuration
    static class CyclicConfig {
        @Bean
        Cyclic cyclic() {
            return new Cyclic();
        }
    }

    static class Cyclic {
        @DisruptorStage(pipeline = "cyc", name = "a", after = "b")
        public void a(EvA e) {
        }

        @DisruptorStage(pipeline = "cyc", name = "b", after = "a")
        public void b(EvA e) {
        }
    }

    @Test
    void cyclicStagesFailFast() {
        runner.withUserConfiguration(CyclicConfig.class)
                .run(ctx -> assertTrue(startupFailedWithIllegalState(ctx), "阶段环应启动失败"));
    }

    // ---- 缺失依赖 ----
    @Configuration
    static class MissingDepConfig {
        @Bean
        MissingDep missingDep() {
            return new MissingDep();
        }
    }

    static class MissingDep {
        @DisruptorStage(pipeline = "mis", name = "a", after = "ghost")
        public void a(EvA e) {
        }
    }

    @Test
    void missingDependencyFailsFast() {
        runner.withUserConfiguration(MissingDepConfig.class)
                .run(ctx -> assertTrue(startupFailedWithIllegalState(ctx), "依赖不存在阶段应启动失败"));
    }

    // ---- 未声明类型发布（运行时） ----
    @Configuration
    static class OkConfig {
        @Bean
        OkStage okStage() {
            return new OkStage();
        }
    }

    static class OkStage {
        @DisruptorStage(pipeline = "ok", name = "s")
        public void h(EvA e) {
        }
    }

    @Test
    void publishUnknownTypeThrows() {
        runner.withUserConfiguration(OkConfig.class).run(ctx -> {
            EventBus bus = ctx.getBean(EventBus.class);
            assertThrows(IllegalArgumentException.class, () -> bus.publish(EvC.class, e -> {
            }), "对未声明管道的事件类型发布应抛 IllegalArgumentException");
        });
    }
}
