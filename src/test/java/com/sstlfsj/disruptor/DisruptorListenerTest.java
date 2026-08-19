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

    static final java.util.List<String> ORDER_TRACE = new java.util.concurrent.CopyOnWriteArrayList<>();
    static final CountDownLatch ORDER_LATCH = new CountDownLatch(2);

    public record PayEvent(String id) {
    }

    @Configuration
    static class OrderedConfig {
        @Bean
        OrderedListeners orderedListeners() {
            return new OrderedListeners();
        }
    }

    // 方法名的字母序与声明顺序都故意与 @Order 期望顺序相反（aaa 名靠前且先声明但 @Order=2 应后执行，
    // zzz 名靠后且后声明但 @Order=1 应先执行），确保「无排序」时无论 getMethods 按字母序还是声明序
    // 都会得到 ["order2","order1"] 而失败，只有 @Order 排序生效才得到期望的 ["order1","order2"]。
    static class OrderedListeners {
        @DisruptorListener
        @org.springframework.core.annotation.Order(2)
        public void aaa(PayEvent e) {
            ORDER_TRACE.add("order2");
            ORDER_LATCH.countDown();
        }

        @DisruptorListener
        @org.springframework.core.annotation.Order(1)
        public void zzz(PayEvent e) {
            ORDER_TRACE.add("order1");
            ORDER_LATCH.countDown();
        }
    }

    @Test
    void listenersRunInOrderAnnotationSequence() {
        runner.withUserConfiguration(OrderedConfig.class).run(ctx -> {
            EventPublisher publisher = ctx.getBean(EventPublisher.class);
            publisher.publish(new PayEvent("P-1"));
            assertTrue(ORDER_LATCH.await(3, TimeUnit.SECONDS), "两个监听器都应被调用");
            assertEquals(java.util.List.of("order1", "order2"), ORDER_TRACE,
                    "应按 @Order 升序调用：order1(@Order 1) 先于 order2(@Order 2)，与方法名/声明序相反");
        });
    }

    @Configuration
    static class InvalidSignatureConfig {
        @Bean
        InvalidListener invalidListener() {
            return new InvalidListener();
        }
    }

    static class InvalidListener {
        @DisruptorListener
        public void twoArgs(OrderEvent e, String extra) {
        }
    }

    @Test
    void nonSingleParamMethodFailsFast() {
        runner.withUserConfiguration(InvalidSignatureConfig.class).run(ctx ->
                org.junit.jupiter.api.Assertions.assertTrue(
                        ctx.getStartupFailure() != null
                                && hasIllegalStateInChain(ctx.getStartupFailure()),
                        "非单参数 @DisruptorListener 方法应导致启动失败(IllegalStateException)"));
    }

    private static boolean hasIllegalStateInChain(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof IllegalStateException) {
                return true;
            }
        }
        return false;
    }
}
