package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoopGroup;
import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopGroupBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopModule;
import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import com.sstlfsj.disruptor.concurrent.internal.EventLoopKernel;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.WorkerSupervisor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DisruptorConcurrentLifecycleTest {

    @Test
    void managesOnlyRootsAndInvokesCallbackAfterEveryRealTermination() throws Exception {
        DisruptorEventLoop standalone = EventLoopBuilder
                .bounded("spring-standalone", 8)
                .build();
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("spring-group", 2, 8)
                .build();
        EventLoop exposedChild = group.select(0);
        DisruptorConcurrentLifecycle lifecycle = new DisruptorConcurrentLifecycle(
                List.<SupervisedScheduledExecutor<?>>of(standalone, group, exposedChild),
                -100,
                Duration.ofSeconds(2));

        assertThat(lifecycle.managedExecutors()).containsExactly(standalone, group);
        assertThat(lifecycle.getPhase()).isEqualTo(-100);
        assertThat(lifecycle.isPauseable()).isFalse();
        lifecycle.start();
        assertThat(lifecycle.isRunning()).isTrue();

        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        standalone.execute(() -> {
            taskEntered.countDown();
            awaitIgnoringInterrupt(releaseTask);
        });
        assertThat(taskEntered.await(2, TimeUnit.SECONDS)).isTrue();
        CountDownLatch callback = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();

        lifecycle.stop(() -> {
            callbackCount.incrementAndGet();
            callback.countDown();
        });

        assertThat(callback.getCount()).isOne();
        ShutdownDeadline shared = supervisorDeadline(standalone);
        assertThat(shared.isBounded()).isTrue();
        for (EventLoop child : group) {
            assertThat(supervisorDeadline(child)).isSameAs(shared);
        }
        releaseTask.countDown();
        assertThat(callback.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackCount).hasValue(1);
        assertThat(lifecycle.isRunning()).isFalse();
        assertThat(standalone.isTerminated()).isTrue();
        assertThat(group.isTerminated()).isTrue();
    }

    @Test
    void startupFailureRollsBackEveryRootBeforePropagatingTheOriginalCause() {
        IllegalStateException original = new IllegalStateException("root start failed");
        DisruptorEventLoop healthy = EventLoopBuilder
                .bounded("healthy-root", 8)
                .build();
        DisruptorEventLoop failing = EventLoopBuilder
                .bounded("failing-root", 8)
                .module(new EventLoopModule() {
                    @Override
                    public void onStart(EventLoop loop) {
                        throw original;
                    }
                })
                .build();
        DisruptorConcurrentLifecycle lifecycle = new DisruptorConcurrentLifecycle(
                List.<SupervisedScheduledExecutor<?>>of(healthy, failing),
                0,
                Duration.ofSeconds(2));

        assertThatThrownBy(lifecycle::start).isSameAs(original);
        assertThat(healthy.isTerminated()).isTrue();
        assertThat(failing.isTerminated()).isTrue();
        assertThat(lifecycle.isRunning()).isFalse();
    }

    private static ShutdownDeadline supervisorDeadline(EventLoop child) throws Exception {
        Object kernel = fieldValue(child, EventLoopKernel.class);
        WorkerSupervisor supervisor = (WorkerSupervisor) fieldValue(kernel, WorkerSupervisor.class);
        Field deadline = WorkerSupervisor.class.getDeclaredField("shutdownDeadline");
        deadline.setAccessible(true);
        return (ShutdownDeadline) deadline.get(supervisor);
    }

    private static Object fieldValue(Object owner, Class<?> fieldType) throws Exception {
        Class<?> type = owner.getClass();
        while (type != null) {
            for (Field field : type.getDeclaredFields()) {
                if (field.getType() == fieldType) {
                    field.setAccessible(true);
                    return field.get(owner);
                }
            }
            type = type.getSuperclass();
        }
        throw new AssertionError("未找到字段：" + fieldType.getName());
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        while (latch.getCount() != 0) {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                // 真实终止由测试显式释放。
            }
        }
    }
}
