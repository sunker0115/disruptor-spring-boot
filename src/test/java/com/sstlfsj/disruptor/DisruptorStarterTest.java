package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.config.DisruptorProperties;
import com.sstlfsj.disruptor.event.ConsumerRegistry;
import com.sstlfsj.disruptor.event.EventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 黑盒验收测试（starter 的对外契约，不触碰 Disruptor 内部 API）：
 * publish→consume、按事件类型路由、配置绑定、自动装配三件套。
 * 实现（src/main）由运行时补齐，测试即「做对」的定义。
 */
class DisruptorStarterTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    public record OrderEvent(String id) {
    }

    @Test
    void autoConfigurationProvidesPublisherAndRegistryBeans() {
        runner.run(ctx -> {
            assertNotNull(ctx.getBean(EventPublisher.class), "EventPublisher bean 应由自动装配提供");
            assertNotNull(ctx.getBean(ConsumerRegistry.class), "ConsumerRegistry bean 应由自动装配提供");
        });
    }

    @Test
    void publishDeliversPayloadToSubscribedConsumer() {
        runner.run(ctx -> {
            ConsumerRegistry registry = ctx.getBean(ConsumerRegistry.class);
            EventPublisher publisher = ctx.getBean(EventPublisher.class);

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> received = new AtomicReference<>();
            registry.subscribe(String.class, msg -> {
                received.set(msg);
                latch.countDown();
            });

            publisher.publish("hello-disruptor");

            assertTrue(latch.await(3, TimeUnit.SECONDS), "消费者应在超时内异步收到事件");
            assertEquals("hello-disruptor", received.get());
        });
    }

    @Test
    void routesEventsByRuntimeTypeOnly() {
        runner.run(ctx -> {
            ConsumerRegistry registry = ctx.getBean(ConsumerRegistry.class);
            EventPublisher publisher = ctx.getBean(EventPublisher.class);

            CountDownLatch orderLatch = new CountDownLatch(1);
            AtomicReference<OrderEvent> gotOrder = new AtomicReference<>();
            CountDownLatch stringLatch = new CountDownLatch(1);

            registry.subscribe(OrderEvent.class, order -> {
                gotOrder.set(order);
                orderLatch.countDown();
            });
            registry.subscribe(String.class, s -> stringLatch.countDown());

            publisher.publish(new OrderEvent("A-1"));

            assertTrue(orderLatch.await(3, TimeUnit.SECONDS), "OrderEvent 消费者应收到事件");
            assertEquals("A-1", gotOrder.get().id());
            assertFalse(stringLatch.await(300, TimeUnit.MILLISECONDS),
                    "String 消费者不应因发布 OrderEvent 而触发（按类型路由）");
        });
    }

    @Test
    void bindsDisruptorConfigurationProperties() {
        runner.withPropertyValues("disruptor.buffer-size=2048", "disruptor.wait-strategy=BLOCKING")
                .run(ctx -> {
                    DisruptorProperties props = ctx.getBean(DisruptorProperties.class);
                    assertEquals(2048, props.getBufferSize(), "buffer-size 应绑定自 disruptor.buffer-size");
                });
    }
}
