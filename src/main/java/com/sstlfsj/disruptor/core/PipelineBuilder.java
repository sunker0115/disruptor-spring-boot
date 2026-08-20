package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 纯逻辑的管道构建器（无 Spring 依赖）：给定一条 {@link EventPipeline} 定义与 {@link DisruptorConfig}，
 * 建立强类型 {@link Disruptor}（预分配 {@code E::new}）、按 name/after 编排 DAG、并行分片、
 * {@link Resettable} 事件叶子后 cleanup，产出 {@link DisruptorPipeline}（不 start）。
 *
 * <p>只认 {@link EventPipeline} 定义（handler 为 {@link Consumer}），因此声明式（注解扫描）与
 * 编程式（builder）两种来源都可复用它——只要在上层转成 EventPipeline 即可。</p>
 */
public class PipelineBuilder {

    private final DisruptorConfig config;

    public PipelineBuilder(DisruptorConfig config) {
        this.config = config;
    }

    public <E> DisruptorPipeline<E> build(EventPipeline<E> pipeline) {
        Class<E> eventType = pipeline.eventType();

        Map<String, List<String>> stageAfter = new LinkedHashMap<>();
        Map<String, EventPipeline.Stage<E>> byName = new LinkedHashMap<>();
        for (EventPipeline.Stage<E> stage : pipeline.stages()) {
            if (byName.put(stage.name(), stage) != null) {
                throw new IllegalStateException("管道 '" + pipeline.pipeline() + "' 阶段名重复：'" + stage.name() + "'");
            }
            stageAfter.put(stage.name(), stage.after());
        }
        PipelineTopology topology = PipelineTopology.build(stageAfter);

        Disruptor<Object> disruptor = createDisruptor(pipeline.pipeline(), eventType);
        disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());

        Map<String, EventHandlerGroup<Object>> groups = new HashMap<>();
        for (String stageName : topology.order()) {
            EventHandler<Object>[] handlers = buildHandlers(byName.get(stageName));
            List<String> deps = topology.dependenciesOf(stageName);
            EventHandlerGroup<Object> group;
            if (deps.isEmpty()) {
                group = disruptor.handleEventsWith(handlers);
            } else {
                EventHandlerGroup<Object> barrier = null;
                for (String dep : deps) {
                    barrier = (barrier == null) ? groups.get(dep) : barrier.and(groups.get(dep));
                }
                group = barrier.handleEventsWith(handlers);
            }
            groups.put(stageName, group);
        }

        if (Resettable.class.isAssignableFrom(eventType)) {
            EventHandlerGroup<Object> allLeaves = null;
            for (String leaf : topology.leaves()) {
                allLeaves = (allLeaves == null) ? groups.get(leaf) : allLeaves.and(groups.get(leaf));
            }
            if (allLeaves != null) {
                allLeaves.handleEventsWith((event, seq, endOfBatch) -> ((Resettable) event).reset());
            }
        }

        return new DisruptorPipeline<>(eventType, disruptor);
    }

    private Disruptor<Object> createDisruptor(String pipelineName, Class<?> eventType) {
        Constructor<?> ctor;
        try {
            ctor = eventType.getDeclaredConstructor();
            ctor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("事件类型 " + eventType.getName()
                    + " 缺少可访问的无参构造，无法预分配（Disruptor 要求预分配事件）", e);
        }
        EventFactory<Object> factory = () -> {
            try {
                return ctor.newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("预分配事件失败：" + eventType.getName(), e);
            }
        };
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger idx = new AtomicInteger();

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "disruptor-" + pipelineName + "-" + idx.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        };
        return new Disruptor<>(factory, config.bufferSize(), threadFactory,
                ProducerType.MULTI, config.createWaitStrategy());
    }

    @SuppressWarnings("unchecked")
    private <E> EventHandler<Object>[] buildHandlers(EventPipeline.Stage<E> stage) {
        int parallelism = Math.max(1, stage.parallelism());
        Consumer<E> handler = stage.handler();
        EventHandler<Object>[] handlers = new EventHandler[parallelism];
        for (int shard = 0; shard < parallelism; shard++) {
            int shardId = shard;
            int n = parallelism;
            handlers[shard] = (event, sequence, endOfBatch) -> {
                if (n > 1 && shardOf(event, sequence, n) != shardId) {
                    return;
                }
                handler.accept((E) event);
            };
        }
        return handlers;
    }

    private static int shardOf(Object event, long sequence, int n) {
        if (event instanceof ShardKeyed keyed) {
            Object key = keyed.shardKey();
            return Math.floorMod(key == null ? 0 : key.hashCode(), n);
        }
        return (int) Math.floorMod(sequence, n);
    }
}
