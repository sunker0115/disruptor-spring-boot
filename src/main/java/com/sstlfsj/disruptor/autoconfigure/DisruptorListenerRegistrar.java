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
 * {@link DisruptorListener} 标注方法，并注册到 {@link ConsumerRegistry}。
 */
public class DisruptorListenerRegistrar implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(DisruptorListenerRegistrar.class);

    private final ConsumerRegistry consumerRegistry;
    private final ConfigurableListableBeanFactory beanFactory;

    public DisruptorListenerRegistrar(ConsumerRegistry consumerRegistry,
                                      ConfigurableListableBeanFactory beanFactory) {
        this.consumerRegistry = consumerRegistry;
        this.beanFactory = beanFactory;
    }

    @Override
    public void afterSingletonsInstantiated() {
        // 先收集全部标注方法，再按事件类型分组、组内按 @Order 升序，最后依次注册，
        // 使 ConsumerRegistry 的追加顺序 = 期望调用顺序。
        Map<Class<?>, List<ListenerMethod>> grouped = new LinkedHashMap<>();
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
                if (AnnotatedElementUtils.findMergedAnnotation(method, DisruptorListener.class) == null) {
                    continue;
                }
                if (method.getParameterCount() != 1) {
                    throw new IllegalStateException(
                            "@DisruptorListener 方法必须恰好一个参数：" + method.getDeclaringClass().getName()
                                    + "#" + method.getName() + " 有 " + method.getParameterCount() + " 个参数");
                }
                Class<?> eventType = method.getParameterTypes()[0];
                Object bean = beanFactory.getBean(beanName);
                grouped.computeIfAbsent(eventType, k -> new ArrayList<>())
                        .add(new ListenerMethod(bean, method, orderOf(method)));
            }
        }

        int count = 0;
        for (Map.Entry<Class<?>, List<ListenerMethod>> entry : grouped.entrySet()) {
            List<ListenerMethod> methods = entry.getValue();
            methods.sort(Comparator.comparingInt(ListenerMethod::order));
            for (ListenerMethod lm : methods) {
                register(entry.getKey(), lm.bean(), lm.method());
                count++;
            }
        }
        log.debug("已注册 {} 个 @DisruptorListener 监听方法", count);
    }

    private static int orderOf(Method method) {
        Order order = AnnotatedElementUtils.findMergedAnnotation(method, Order.class);
        return order != null ? order.value() : Ordered.LOWEST_PRECEDENCE;
    }

    private record ListenerMethod(Object bean, Method method, int order) {
    }

    @SuppressWarnings("unchecked")
    private void register(Class<?> eventType, Object bean, Method method) {
        ReflectionUtils.makeAccessible(method);
        Consumer<Object> consumer = payload -> ReflectionUtils.invokeMethod(method, bean, payload);
        consumerRegistry.subscribe((Class<Object>) eventType, consumer);
    }
}
