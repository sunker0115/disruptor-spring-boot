package com.sstlfsj.disruptor.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 编程式管道定义——声明式 {@link DisruptorStage} 的等价替代。在 {@code @Configuration} 里用
 * fluent builder 定义一条强类型管道并作为 {@code @Bean} 暴露；starter 会与注解式统一收集，
 * 按 name/after 编排为同一套 DAG（共用 PipelineTopology）。handler 是内联 {@link Consumer}，
 * 直接调用（无反射）。
 *
 * <pre>{@code
 * @Bean
 * EventPipeline<OrderEvent> orderPipeline(OrderService svc) {
 *     return EventPipeline.builder("order", OrderEvent.class)
 *         .stage("validate", svc::validate)
 *         .stage("persist", svc::persist).after("validate")
 *         .stage("audit",   svc::audit).after("validate")
 *         .stage("notify",  svc::notify).after("persist", "audit")
 *         .parallelism("persist", 4)
 *         .build();
 * }
 * }</pre>
 *
 * @param <E> 强类型事件类型
 */
public final class EventPipeline<E> {

    private final String pipeline;
    private final Class<E> eventType;
    private final List<Stage<E>> stages;

    private EventPipeline(String pipeline, Class<E> eventType, List<Stage<E>> stages) {
        this.pipeline = pipeline;
        this.eventType = eventType;
        this.stages = stages;
    }

    public String pipeline() {
        return pipeline;
    }

    public Class<E> eventType() {
        return eventType;
    }

    public List<Stage<E>> stages() {
        return stages;
    }

    public static <E> Builder<E> builder(String pipeline, Class<E> eventType) {
        return new Builder<>(pipeline, eventType);
    }

    /** 不可变的单阶段定义。 */
    public static final class Stage<E> {
        private final String name;
        private final List<String> after;
        private final int parallelism;
        private final Consumer<E> handler;

        Stage(String name, List<String> after, int parallelism, Consumer<E> handler) {
            this.name = name;
            this.after = after;
            this.parallelism = parallelism;
            this.handler = handler;
        }

        public String name() {
            return name;
        }

        public List<String> after() {
            return after;
        }

        public int parallelism() {
            return parallelism;
        }

        public Consumer<E> handler() {
            return handler;
        }
    }

    /** fluent builder：{@code .stage(...).after(...)} 链式声明阶段与依赖。 */
    public static final class Builder<E> {

        private final String pipeline;
        private final Class<E> eventType;
        private final List<MutableStage<E>> stages = new ArrayList<>();
        private final Map<String, MutableStage<E>> byName = new HashMap<>();

        Builder(String pipeline, Class<E> eventType) {
            this.pipeline = pipeline;
            this.eventType = eventType;
        }

        /** 添加一个阶段（默认无依赖、parallelism=1）。后续 {@link #after} 作用于此阶段。 */
        public Builder<E> stage(String name, Consumer<E> handler) {
            if (byName.containsKey(name)) {
                throw new IllegalStateException("管道 '" + pipeline + "' 阶段名重复：'" + name + "'");
            }
            MutableStage<E> stage = new MutableStage<>(name, handler);
            stages.add(stage);
            byName.put(name, stage);
            return this;
        }

        /** 为最近添加的阶段声明上游依赖。 */
        public Builder<E> after(String... deps) {
            if (stages.isEmpty()) {
                throw new IllegalStateException("after() 必须跟在 stage() 之后");
            }
            stages.get(stages.size() - 1).after.addAll(List.of(deps));
            return this;
        }

        /** 设置指定阶段的并行分片数。 */
        public Builder<E> parallelism(String stageName, int parallelism) {
            MutableStage<E> stage = byName.get(stageName);
            if (stage == null) {
                throw new IllegalStateException("未知阶段：'" + stageName + "'");
            }
            stage.parallelism = parallelism;
            return this;
        }

        public EventPipeline<E> build() {
            List<Stage<E>> built = new ArrayList<>();
            for (MutableStage<E> s : stages) {
                built.add(new Stage<>(s.name, List.copyOf(s.after), s.parallelism, s.handler));
            }
            return new EventPipeline<>(pipeline, eventType, List.copyOf(built));
        }

        private static final class MutableStage<E> {
            final String name;
            final Consumer<E> handler;
            final List<String> after = new ArrayList<>();
            int parallelism = 1;

            MutableStage(String name, Consumer<E> handler) {
                this.name = name;
                this.handler = handler;
            }
        }
    }
}
