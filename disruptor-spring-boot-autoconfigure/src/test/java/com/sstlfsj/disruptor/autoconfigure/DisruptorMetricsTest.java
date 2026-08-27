package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineSpec;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class DisruptorMetricsTest {

    @Test
    void reflectsRuntimeLifecycleAndConsumerBacklog() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch handled = new CountDownLatch(1);
        PipelineSpec<TestEvent> spec = PipelineSpec.builder(
                        "orders", TestEvent.class, TestEvent::new)
                .bufferSize(16)
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                    entered.countDown();
                    release.await();
                    handled.countDown();
                }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new DisruptorMetrics(runtime).bindTo(registry);
        Gauge running = registry.get("disruptor.runtime.running").gauge();
        Gauge backlog = registry.get("disruptor.pipeline.backlog")
                .tag("pipeline", "orders").gauge();

        assertThat(running.value()).isZero();
        runtime.start();
        try {
            assertThat(running.value()).isEqualTo(1.0);
            runtime.require("orders", TestEvent.class).publish(event -> {
            });
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(backlog.value()).isEqualTo(1.0);
            release.countDown();
            assertThat(handled.await(2, TimeUnit.SECONDS)).isTrue();
            awaitGauge(backlog, 0.0);
        } finally {
            release.countDown();
            runtime.shutdown();
        }
        assertThat(running.value()).isZero();
    }

    private static void awaitGauge(Gauge gauge, double expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (gauge.value() != expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(gauge.value()).isEqualTo(expected);
    }

    static final class TestEvent {
    }
}
