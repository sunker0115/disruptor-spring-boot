package com.sstlfsj.disruptor.example.concurrent;

import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentAutoConfiguration;
import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentLifecycle;
import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.CancellationSource;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoopGroup;
import com.sstlfsj.disruptor.concurrent.EventLoopScheduledFuture;
import com.sstlfsj.disruptor.example.DemoResults;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.boot.CommandLineRunner;

import static org.assertj.core.api.Assertions.assertThat;

class ConcurrentExampleTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    DisruptorConcurrentAutoConfiguration.class))
            .withUserConfiguration(ConcurrentExampleConfiguration.class);

    @Test
    void runsTheDocumentedConcurrentCapabilitiesAndStopsOnlyThroughSpringOwner() {
        contextRunner.run(context -> {
            OrderEventLoopService service = context.getBean(OrderEventLoopService.class);
            DisruptorEventLoop orders = context.getBean(
                    "orderEventLoop", DisruptorEventLoop.class);
            DisruptorEventLoopGroup workers = context.getBean(
                    DisruptorEventLoopGroup.class);
            DisruptorConcurrentLifecycle lifecycle = context.getBean(
                    DisruptorConcurrentLifecycle.class);

            assertThat(service.submitOrder("order-42").get(2, TimeUnit.SECONDS))
                    .isEqualTo("order-42@tenant-a");

            AtomicInteger fixedRateRuns = new AtomicInteger();
            ScheduledFuture<?> fixedRate = service.startFixedRate(
                    fixedRateRuns::incrementAndGet);
            awaitAtLeast(fixedRateRuns, 2);
            assertThat(fixedRate.cancel(false)).isTrue();

            AtomicInteger dynamicRuns = new AtomicInteger();
            EventLoopScheduledFuture<Void> dynamic = service.startDynamicDelay(
                    dynamicRuns::incrementAndGet);
            awaitDone(dynamic);
            assertThat(dynamicRuns).hasValue(3);
            assertThat(dynamic.snapshot().executions()).isEqualTo(3);

            CancellationSource cancellation = new CancellationSource();
            var observedReason = service.observeCancellation(cancellation);
            CancellationReason reason = CancellationReason.of("client-left");
            assertThat(cancellation.cancel(reason)).isTrue();
            assertThat(observedReason.toCompletableFuture().get(2, TimeUnit.SECONDS))
                    .isSameAs(reason);

            String firstWorker = service.affinityWorker(7).get(2, TimeUnit.SECONDS);
            String sameWorker = service.affinityWorker(7).get(2, TimeUnit.SECONDS);
            String otherWorker = service.affinityWorker(8).get(2, TimeUnit.SECONDS);
            assertThat(sameWorker).isEqualTo(firstWorker);
            assertThat(otherWorker).isNotEqualTo(firstWorker);

            lifecycle.stop();
            assertThat(orders.termination().toCompletableFuture()).isCompleted();
            assertThat(workers.termination().toCompletableFuture()).isCompleted();
            assertThat(workers).allMatch(child -> child.isTerminated());
        });
    }

    @Test
    void springRunnerCompletesTheDocumentedConcurrentPath() throws Exception {
        Class<?> runnerType = Class.forName(
                "com.sstlfsj.disruptor.example.concurrent.ConcurrentDemoRunner");
        contextRunner
                .withBean(DemoResults.class)
                .withBean(runnerType)
                .run(context -> {
                    CommandLineRunner runner = (CommandLineRunner) context.getBean(runnerType);
                    runner.run();

                    assertThat(context.getBean(DemoResults.class).isDone("concurrent")).isTrue();
                });
    }

    private static void awaitAtLeast(AtomicInteger actual, int expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (actual.get() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(actual.get()).isGreaterThanOrEqualTo(expected);
    }

    private static void awaitDone(EventLoopScheduledFuture<?> future) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (!future.isDone() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(future.isDone()).isTrue();
    }

}
