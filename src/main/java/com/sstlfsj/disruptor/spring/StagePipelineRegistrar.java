package com.sstlfsj.disruptor.spring;

import com.sstlfsj.disruptor.core.EventPipeline;
import com.sstlfsj.disruptor.core.PipelineBuilder;
import com.sstlfsj.disruptor.core.Pipelines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 从 Spring 容器收集管道定义，交给 core 的 {@link PipelineBuilder} 统一构建（本类是 core 与 Spring
 * 的桥接，依赖 spring-context/beans/core，但不依赖 Spring Boot）：
 * <ul>
 *   <li>声明式：扫描全部 bean 的 {@link DisruptorStage} 方法，转成 {@link EventPipeline}
 *       （handler 为反射调用）；</li>
 *   <li>编程式：收集所有 {@link EventPipeline} bean。</li>
 * </ul>
 * 管道名跨来源唯一，逐条 {@code build} 并注册到 {@link Pipelines}（事件类型唯一性由 Pipelines 校验）。
 */
public class StagePipelineRegistrar implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(StagePipelineRegistrar.class);

    private final ConfigurableListableBeanFactory beanFactory;
    private final PipelineBuilder pipelineBuilder;
    private final Pipelines pipelines;

    public StagePipelineRegistrar(ConfigurableListableBeanFactory beanFactory,
                                  PipelineBuilder pipelineBuilder,
                                  Pipelines pipelines) {
        this.beanFactory = beanFactory;
        this.pipelineBuilder = pipelineBuilder;
        this.pipelines = pipelines;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, EventPipeline<?>> defs = new LinkedHashMap<>();
        collectAnnotated(defs);
        collectProgrammatic(defs);
        for (EventPipeline<?> def : defs.values()) {
            pipelines.register(pipelineBuilder.build(def));
        }
        log.debug("已建立 {} 条 Disruptor 管道", defs.size());
    }

    private void collectAnnotated(Map<String, EventPipeline<?>> defs) {
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
                    throw new IllegalStateException("@DisruptorStage 方法必须恰好一个参数："
                            + method.getDeclaringClass().getName() + "#" + method.getName());
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
            EventPipeline.Builder<Object> builder = EventPipeline.builder(pipeline, cast(eventType));
            for (StageMethod sm : methods) {
                if (sm.eventType() != eventType) {
                    throw new IllegalStateException("管道 '" + pipeline + "' 各阶段事件类型不一致："
                            + eventType.getName() + " vs " + sm.eventType().getName());
                }
                Method m = sm.method();
                Object bean = sm.bean();
                ReflectionUtils.makeAccessible(m);
                Consumer<Object> handler = event -> ReflectionUtils.invokeMethod(m, bean, event);
                builder.stage(sm.ann().name(), handler).after(sm.ann().after());
                if (sm.ann().parallelism() > 1) {
                    builder.parallelism(sm.ann().name(), sm.ann().parallelism());
                }
            }
            putDef(defs, pipeline, builder.build());
        }
    }

    private void collectProgrammatic(Map<String, EventPipeline<?>> defs) {
        Map<String, EventPipeline> beans = beanFactory.getBeansOfType(EventPipeline.class);
        for (EventPipeline<?> ep : beans.values()) {
            putDef(defs, ep.pipeline(), ep);
        }
    }

    private void putDef(Map<String, EventPipeline<?>> defs, String pipeline, EventPipeline<?> def) {
        if (defs.put(pipeline, def) != null) {
            throw new IllegalStateException("管道名重复：'" + pipeline + "'（声明式与编程式不能同名）");
        }
    }

    @SuppressWarnings("unchecked")
    private static <E> Class<E> cast(Class<?> type) {
        return (Class<E>) type;
    }

    private record StageMethod(DisruptorStage ann, Object bean, Method method, Class<?> eventType) {
    }
}
