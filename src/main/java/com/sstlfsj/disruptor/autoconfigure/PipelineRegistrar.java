package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.event.DisruptorStage;
import com.sstlfsj.disruptor.event.EventPipeline;
import com.sstlfsj.disruptor.event.LoggingExceptionHandler;
import com.sstlfsj.disruptor.event.Resettable;
import com.sstlfsj.disruptor.event.ShardKeyed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 在所有单例初始化后，从两种来源收集管道定义并统一建立强类型 Disruptor 管道：
 * <ul>
 *   <li>声明式：扫描全部 bean 的 {@link DisruptorStage} 方法（反射调用）；</li>
 *   <li>编程式：收集所有 {@link EventPipeline} bean（内联 Consumer 直接调用）。</li>
 * </ul>
 * 两者共用 {@link PipelineTopology} 校验 DAG、用 {@code then/and} 编排，注册到 {@link Pipelines}
 * （不启动，启动由 {@link DisruptorLifecycle} 负责）。管道名与事件类型均全局唯一。
 */
public class PipelineRegistrar implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(PipelineRegistrar.class);

    private final ConfigurableListableBeanFactory beanFactory;
    private final DisruptorProperties properties;
    private final Pipelines pipelines;

    public PipelineRegistrar(ConfigurableListableBeanFactory beanFactory,
                             DisruptorProperties properties,
                             Pipelines pipelines) {
        this.beanFactory = beanFactory;
        this.properties = properties;
        this.pipelines = pipelines;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, PipelineDef> defs = new LinkedHashMap<>();
        collectAnnotated(defs);
        collectProgrammatic(defs);

        Map<Class<?>, String> typeToPipeline = new HashMap<>();
        for (Map.Entry<String, PipelineDef> entry : defs.entrySet()) {
            buildPipeline(entry.getKey(), entry.getValue(), typeToPipeline);
        }
        log.debug("已建立 {} 条 Disruptor 管道", defs.size());
    }

    // ---- 声明式来源：@DisruptorStage ----

    private void collectAnnotated(Map<String, PipelineDef> defs) {
        Map<String, List<StageMethod>> byPipeline = new LinkedHashMap<>();
        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            if (beanFactory.getBeanDefinition(beanName).isAbstract()) {
                continue;
            }
            Class<?> beanType = beanFactory.getType(beanName);
            if (beanType == null) {
                continue;
            }
            Class<?> userType = ClassUtils.getUserClass(beanType);
            for (Method method : userType.getMethods()) {
                if (method.isBridge() || method.isSynthetic()) {
                    continue;
                }
                DisruptorStage ann = AnnotatedElementUtils.findMergedAnnotation(method, DisruptorStage.class);
                if (ann == null) {
                    continue;
                }
                if (method.getParameterCount() != 1) {
                    throw new IllegalStateException("@DisruptorStage 方法必须恰好一个参数：" + describe(method));
                }
                Object bean = beanFactory.getBean(beanName);
                byPipeline.computeIfAbsent(ann.pipeline(), k -> new ArrayList<>())
                        .add(new StageMethod(ann, bean, method, method.getParameterTypes()[0]));
            }
        }

        for (Map.Entry<String, List<StageMethod>> entry : byPipeline.entrySet()) {
            String pipeline = entry.getKey();
            List<StageMethod> methods = entry.getValue();
            Class<?> eventType = methods.get(0).eventType();
            List<ResolvedStage> stages = new ArrayList<>();
            for (StageMethod sm : methods) {
                if (sm.eventType() != eventType) {
                    throw new IllegalStateException("管道 '" + pipeline + "' 各阶段事件类型不一致："
                            + eventType.getName() + " vs " + sm.eventType().getName());
                }
                Method method = sm.method();
                Object bean = sm.bean();
                ReflectionUtils.makeAccessible(method);
                EventInvoker invoker = event -> ReflectionUtils.invokeMethod(method, bean, event);
                stages.add(new ResolvedStage(sm.ann().name(), List.of(sm.ann().after()),
                        Math.max(1, sm.ann().parallelism()), invoker));
            }
            putDef(defs, pipeline, new PipelineDef(eventType, stages));
        }
    }

    // ---- 编程式来源：EventPipeline bean ----

    private void collectProgrammatic(Map<String, PipelineDef> defs) {
        Map<String, EventPipeline> beans = beanFactory.getBeansOfType(EventPipeline.class);
        for (EventPipeline<?> ep : beans.values()) {
            List<ResolvedStage> stages = new ArrayList<>();
            for (EventPipeline.Stage<?> stage : ep.stages()) {
                @SuppressWarnings("unchecked")
                Consumer<Object> handler = (Consumer<Object>) stage.handler();
                EventInvoker invoker = handler::accept;
                stages.add(new ResolvedStage(stage.name(), stage.after(),
                        Math.max(1, stage.parallelism()), invoker));
            }
            putDef(defs, ep.pipeline(), new PipelineDef(ep.eventType(), stages));
        }
    }

    private void putDef(Map<String, PipelineDef> defs, String pipeline, PipelineDef def) {
        if (defs.put(pipeline, def) != null) {
            throw new IllegalStateException("管道名重复：'" + pipeline + "'（声明式与编程式不能同名）");
        }
    }

    // ---- 统一建管道 ----

    private void buildPipeline(String pipelineName, PipelineDef def, Map<Class<?>, String> typeToPipeline) {
        Class<?> eventType = def.eventType();
        String existing = typeToPipeline.put(eventType, pipelineName);
        if (existing != null) {
            throw new IllegalStateException("事件类型 " + eventType.getName()
                    + " 被多个管道使用：'" + existing + "' 与 '" + pipelineName
                    + "'；一种事件类型仅允许一条管道");
        }

        Map<String, List<String>> stageAfter = new LinkedHashMap<>();
        Map<String, ResolvedStage> byName = new LinkedHashMap<>();
        for (ResolvedStage stage : def.stages()) {
            if (byName.put(stage.name(), stage) != null) {
                throw new IllegalStateException("管道 '" + pipelineName + "' 阶段名重复：'" + stage.name() + "'");
            }
            stageAfter.put(stage.name(), stage.after());
        }
        PipelineTopology topology = PipelineTopology.build(stageAfter);

        Disruptor<Object> disruptor = createDisruptor(pipelineName, eventType);
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

        pipelines.register(new DisruptorPipeline<>(cast(eventType), disruptor));
    }

    private Disruptor<Object> createDisruptor(String pipelineName, Class<?> eventType) {
        Constructor<?> ctor;
        try {
            ctor = eventType.getDeclaredConstructor();
            ReflectionUtils.makeAccessible(ctor);
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
        WaitStrategy waitStrategy = properties.createWaitStrategy();
        return new Disruptor<>(factory, properties.getBufferSize(), threadFactory,
                ProducerType.MULTI, waitStrategy);
    }

    @SuppressWarnings("unchecked")
    private EventHandler<Object>[] buildHandlers(ResolvedStage stage) {
        int parallelism = stage.parallelism();
        EventInvoker invoker = stage.invoker();
        EventHandler<Object>[] handlers = new EventHandler[parallelism];
        for (int shard = 0; shard < parallelism; shard++) {
            int shardId = shard;
            int n = parallelism;
            handlers[shard] = (event, sequence, endOfBatch) -> {
                if (n > 1 && shardOf(event, sequence, n) != shardId) {
                    return;
                }
                invoker.invoke(event);
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

    private static String describe(Method method) {
        return method.getDeclaringClass().getName() + "#" + method.getName();
    }

    @SuppressWarnings("unchecked")
    private static <E> Class<E> cast(Class<?> type) {
        return (Class<E>) type;
    }

    @FunctionalInterface
    private interface EventInvoker {
        void invoke(Object event);
    }

    private record ResolvedStage(String name, List<String> after, int parallelism, EventInvoker invoker) {
    }

    private record PipelineDef(Class<?> eventType, List<ResolvedStage> stages) {
    }

    private record StageMethod(DisruptorStage ann, Object bean, Method method, Class<?> eventType) {
    }
}
