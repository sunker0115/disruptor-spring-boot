package com.sstlfsj.disruptor.event;

import org.springframework.aot.hint.annotation.Reflective;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在 Spring bean 的方法上，声明式订阅事件总线。方法必须恰好一个参数，
 * 该参数类型即监听的事件类型；容器启动时由 {@link DisruptorListenerRegistrar}
 * 自动注册到 {@link ConsumerRegistry}。
 *
 * <p>同一事件类型有多个监听器时，可配合 {@code @org.springframework.core.annotation.Order}
 * 控制调用顺序（值越小越先）。</p>
 *
 * <p>用 {@link Reflective} 元注解标注，使 Spring AOT 在构建期为标注方法注册反射
 * INVOKE hints，从而兼容 GraalVM native image。</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Reflective
public @interface DisruptorListener {

    /**
     * 所属处理阶段名。空串（默认）归入隐式的 {@code default} 阶段。
     * 非空时该阶段必须在 {@code disruptor.pipeline} 中声明，否则启动失败。
     */
    String stage() default "";
}
