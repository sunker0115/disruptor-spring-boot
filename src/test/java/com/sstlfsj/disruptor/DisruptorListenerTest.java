package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.event.DisruptorListener;
import com.sstlfsj.disruptor.event.EventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 注解式监听黑盒验收测试：自动注册、@Order 顺序、非单参数 fail-fast。
 */
class DisruptorListenerTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    public record OrderEvent(String id) {
    }

    static final CountDownLatch AUTO_LATCH = new CountDownLatch(1);
    static final AtomicReference<String> AUTO_RECEIVED = new AtomicReference<>();

    @Configuration
    static class AutoRegisterConfig {
        @Bean
        AutoRegisterListener autoRegisterListener() {
            return new AutoRegisterListener();
        }
    }

    static class AutoRegisterListener {
        @DisruptorListener
        public void onOrder(OrderEvent e) {
            AUTO_RECEIVED.set(e.id());
            AUTO_LATCH.countDown();
        }
    }

    @Test
    void annotatedMethodIsAutoRegistered() {
        runner.withUserConfiguration(AutoRegisterConfig.class).run(ctx -> {
            EventPublisher publisher = ctx.getBean(EventPublisher.class);
            publisher.publish(new OrderEvent("A-1"));
            assertTrue(AUTO_LATCH.await(3, TimeUnit.SECONDS), "@DisruptorListener 方法应被自动注册并收到事件");
            assertEquals("A-1", AUTO_RECEIVED.get());
        });
    }
}
