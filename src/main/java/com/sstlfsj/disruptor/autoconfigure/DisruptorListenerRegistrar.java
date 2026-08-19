package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.event.ConsumerRegistry;
import com.sstlfsj.disruptor.event.DisruptorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.Order;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 在所有单例初始化后（{@link SmartInitializingSingleton}），扫描容器中所有 bean 的
 * {@link DisruptorListener} 标注方法，按 (阶段, 事件类型) 分组、组内按
 * {@code @Order} 升序，注册到对应阶段的 {@link ConsumerRegistry}。
 */
public class DisruptorListenerRegistrar implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(DisruptorListenerRegistrar.class);

    private final StageRegistries stageRegistries;
    private final ConfigurableListableBeanFactory beanFactory;

    public DisruptorListenerRegistrar(StageRegistries stageRegistries,
                                      ConfigurableListableBeanFactory beanFactory) {
        this.stageRegistries = stageRegistries;
        this.beanFactory = beanFactory;
    }

    @Override
    public void afterSingletonsInstantiated() {
        // 收集全部标注方法，按 (阶段,事件类型) 分组、组内按 @Order 升序，再依次注册，
        // 使每个阶段 registry 的追加顺序 = 期望调用顺序。
        Map<StageType, List<ListenerMethod>> grouped = new LinkedHashMap<>();
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
                DisruptorListener annotation =
                        AnnotatedElementUtils.findMergedAnnotation(method, DisruptorListener.class);
                if (annotation == null) {
                    continue;
                }
                if (method.getParameterCount() != 1) {
                    throw new IllegalStateException(
                            "@DisruptorListener 方法必须恰好一个参数：" + method.getDeclaringClass().getName()
                                    + "#" + method.getName() + " 有 " + method.getParameterCount() + " 个参数");
                }
                String stage = resolveStage(annotation.stage());
                if (!stageRegistries.hasStage(stage)) {
                    throw new IllegalStateException(
                            "@DisruptorListener 引用了未声明的阶段 '" + stage + "'（"
                                    + method.getDeclaringClass().getName() + "#" + method.getName()
                                    + "），请在 disruptor.pipeline 中声明该阶段");
                }
                Class<?> eventType = method.getParameterTypes()[0];
                Object bean = beanFactory.getBean(beanName);
                grouped.computeIfAbsent(new StageType(stage, eventType), k -> new ArrayList<>())
                        .add(new ListenerMethod(bean, method, orderOf(method)));
            }
        }

        int count = 0;
        for (Map.Entry<StageType, List<ListenerMethod>> entry : grouped.entrySet()) {
            List<ListenerMethod> methods = entry.getValue();
            methods.sort(Comparator.comparingInt(ListenerMethod::order));
            ConsumerRegistry registry = stageRegistries.forStage(entry.getKey().stage());
            for (ListenerMethod lm : methods) {
                register(registry, entry.getKey().eventType(), lm.bean(), lm.method());
                count++;
            }
        }
        log.debug("已注册 {} 个 @DisruptorListener 监听方法", count);
    }

    private static String resolveStage(String stage) {
        return (stage == null || stage.isEmpty()) ? PipelineTopology.DEFAULT_STAGE : stage;
    }

    private static int orderOf(Method method) {
        Order order = AnnotatedElementUtils.findMergedAnnotation(method, Order.class);
        return order != null ? order.value() : Ordered.LOWEST_PRECEDENCE;
    }

    @SuppressWarnings("unchecked")
    private void register(ConsumerRegistry registry, Class<?> eventType, Object bean, Method method) {
        ReflectionUtils.makeAccessible(method);
        Consumer<Object> consumer = payload -> ReflectionUtils.invokeMethod(method, bean, payload);
        registry.subscribe((Class<Object>) eventType, consumer);
    }

    private record StageType(String stage, Class<?> eventType) {
    }

    private record ListenerMethod(Object bean, Method method, int order) {
    }
}
