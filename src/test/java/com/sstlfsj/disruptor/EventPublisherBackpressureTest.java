package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.event.ConsumerRegistry;
import com.sstlfsj.disruptor.event.EventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 背压能力验收：tryPublish 非阻塞（满时返回 false）、remainingCapacity 反映剩余容量。
 */
class EventPublisherBackpressureTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    @Test
    void tryPublishDeliversWhenCapacityAvailable() {
        runner.run(ctx -> {
            ConsumerRegistry registry = ctx.getBean(ConsumerRegistry.class);
            EventPublisher publisher = ctx.getBean(EventPublisher.class);

            CountDownLatch latch = new CountDownLatch(1);
            registry.subscribe(String.class, s -> latch.countDown());

            assertTrue(publisher.tryPublish("hi"), "有容量时 tryPublish 应返回 true");
            assertTrue(latch.await(3, TimeUnit.SECONDS), "事件应被消费");
        });
    }

    @Test
    void remainingCapacityReflectsBufferSize() {
        runner.withPropertyValues("disruptor.buffer-size=64").run(ctx -> {
            EventPublisher publisher = ctx.getBean(EventPublisher.class);
            assertEquals(64, publisher.remainingCapacity(),
                    "空闲无积压时 remainingCapacity 应等于 bufferSize");
        });
    }

    @Test
    void tryPublishReturnsFalseWhenBufferFull() {
        runner.withPropertyValues("disruptor.buffer-size=8").run(ctx -> {
            ConsumerRegistry registry = ctx.getBean(ConsumerRegistry.class);
            EventPublisher publisher = ctx.getBean(EventPublisher.class);

            // 消费者永久阻塞，占住消费进度，使 ring buffer 必然填满
            CountDownLatch block = new CountDownLatch(1);
            CountDownLatch consuming = new CountDownLatch(1);
            registry.subscribe(String.class, s -> {
                consuming.countDown();
                try {
                    block.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            publisher.publish("first"); // 让消费者取走首个事件并阻塞
            assertTrue(consuming.await(3, TimeUnit.SECONDS), "消费者应已取走首个事件并阻塞");

            boolean sawFalse = false;
            for (int i = 0; i < 100; i++) {
                if (!publisher.tryPublish("x")) {
                    sawFalse = true;
                    break;
                }
            }
            block.countDown(); // 释放消费者，避免关闭挂起
            assertTrue(sawFalse, "buffer 满时 tryPublish 应返回 false（非阻塞背压）");
        });
    }
}
