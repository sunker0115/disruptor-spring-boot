package com.sstlfsj.disruptor.spring;

import org.springframework.aot.hint.annotation.Reflective;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明一个处理阶段。标注在 Spring bean 的方法上，方法签名必须为 {@code void m(E event)}
 * （恰好一个参数），参数类型 {@code E} 即该管道的强类型事件类型；同一 {@link #pipeline()}
 * 下各阶段方法的参数类型必须一致。
 *
 * <p>容器启动时按 {@link #pipeline()} 分组，为每种事件类型建立一个 {@code Disruptor<E>}，
 * 按 {@link #after()} 声明的依赖用 Disruptor 原生 {@code then/and} 编排成 DAG。</p>
 *
 * <p>用 {@link Reflective} 元注解标注，使 Spring AOT 为标注方法注册反射 hints，兼容 native image。</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Reflective
public @interface DisruptorStage {

    /** 所属管道名（同管道各阶段共享一个 Disruptor 与一种事件类型）。 */
    String pipeline();

    /** 阶段名，在所属管道内唯一。 */
    String name();

    /** 依赖的上游阶段名。本阶段在这些上游都处理完同一事件后才处理；空 = 源头阶段。 */
    String[] after() default {};

    /** 阶段并行分片数（各分片一个独立线程）。默认 1。&gt; 1 时每个事件仅由一个分片处理。 */
    int parallelism() default 1;
}
