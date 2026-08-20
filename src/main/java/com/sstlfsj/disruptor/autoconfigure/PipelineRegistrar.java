package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.event.DisruptorStage;
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

/**
 * 在所有单例初始化后，扫描全部 bean 的 {@link DisruptorStage} 方法，按 pipeline 分组，
 * 为每种事件类型建立一个强类型 {@link Disruptor}（预分配 {@code E::new}），按阶段依赖
 * 编排 DAG，并注册到 {@link Pipelines}（不启动，启动由 {@link DisruptorLifecycle} 负责）。
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
        Map<String, List<StageMethod>> byPipeline = scanStages();
        Map<Class<?>, String> typeToPipeline = new HashMap<>();
        for (Map.Entry<String, List<StageMethod>> entry : byPipeline.entrySet()) {
            buildPipeline(entry.getKey(), entry.getValue(), typeToPipeline);
        }
        log.debug("已建立 {} 条 Disruptor 管道", byPipeline.size());
    }

    private Map<String, List<StageMethod>> scanStages() {
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
        return byPipeline;
    }

    private void buildPipeline(String pipelineName, List<StageMethod> stages,
                               Map<Class<?>, String> typeToPipeline) {
        // 事件类型一致性
        Class<?> eventType = stages.get(0).eventType();
        for (StageMethod sm : stages) {
            if (sm.eventType() != eventType) {
                throw new IllegalStateException("管道 '" + pipelineName + "' 各阶段事件类型不一致："
                        + eventType.getName() + " vs " + sm.eventType().getName());
            }
        }
        // 事件类型跨管道唯一
        String existing = typeToPipeline.put(eventType, pipelineName);
        if (existing != null) {
            throw new IllegalStateException("事件类型 " + eventType.getName()
                    + " 被多个管道使用：'" + existing + "' 与 '" + pipelineName
                    + "'；一种事件类型仅允许一条管道");
        }
        // 阶段名唯一 + 拓扑
        Map<String, List<String>> stageAfter = new LinkedHashMap<>();
        Map<String, StageMethod> byStageName = new LinkedHashMap<>();
        for (StageMethod sm : stages) {
            String name = sm.ann().name();
            if (byStageName.put(name, sm) != null) {
                throw new IllegalStateException("管道 '" + pipelineName + "' 阶段名重复：'" + name + "'");
            }
            stageAfter.put(name, List.of(sm.ann().after()));
        }
        PipelineTopology topology = PipelineTopology.build(stageAfter);

        // 建 Disruptor 并编排 DAG
        Disruptor<Object> disruptor = createDisruptor(pipelineName, eventType);
        disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());

        Map<String, EventHandlerGroup<Object>> groups = new HashMap<>();
        for (String stageName : topology.order()) {
            EventHandler<Object>[] handlers = buildHandlers(byStageName.get(stageName));
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

        // 若事件可重置，所有叶子之后接单线程 cleanup handler 重置槽位供复用
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
    private EventHandler<Object>[] buildHandlers(StageMethod sm) {
        int parallelism = Math.max(1, sm.ann().parallelism());
        Method method = sm.method();
        ReflectionUtils.makeAccessible(method);
        Object bean = sm.bean();
        EventHandler<Object>[] handlers = new EventHandler[parallelism];
        for (int shard = 0; shard < parallelism; shard++) {
            int shardId = shard;
            int n = parallelism;
            handlers[shard] = (event, sequence, endOfBatch) -> {
                if (n > 1 && shardOf(event, sequence, n) != shardId) {
                    return;
                }
                ReflectionUtils.invokeMethod(method, bean, event);
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

    private record StageMethod(DisruptorStage ann, Object bean, Method method, Class<?> eventType) {
    }
}
