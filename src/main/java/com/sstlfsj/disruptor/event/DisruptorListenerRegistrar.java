package com.sstlfsj.disruptor.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
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
        int count = 0;
        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            Class<?> type = beanFactory.getType(beanName);
            if (type == null) {
                continue;
            }
            for (Method method : type.getMethods()) {
                if (!method.isAnnotationPresent(DisruptorListener.class)) {
                    continue;
                }
                Class<?> eventType = method.getParameterTypes()[0];
                Object bean = beanFactory.getBean(beanName);
                register(eventType, bean, method);
                count++;
            }
        }
        log.debug("已注册 {} 个 @DisruptorListener 监听方法", count);
    }

    @SuppressWarnings("unchecked")
    private void register(Class<?> eventType, Object bean, Method method) {
        ReflectionUtils.makeAccessible(method);
        Consumer<Object> consumer = payload -> ReflectionUtils.invokeMethod(method, bean, payload);
        consumerRegistry.subscribe((Class<Object>) eventType, consumer);
    }
}
