package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoopGroup;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopGroupBuilder;
import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class DisruptorConcurrentMetricsTest {

    @Test
    void bindsSnapshotGaugesForBoundedRootsAndUnboundedGroupChildren() throws Exception {
        DisruptorEventLoop bounded = EventLoopBuilder
                .bounded("bounded-metrics", 8)
                .build();
        DisruptorEventLoopGroup unboundedGroup = EventLoopGroupBuilder
                .unbounded("metrics-group", 2, 8)
                .build();
        bounded.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        unboundedGroup.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        try {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            DisruptorConcurrentMetrics metrics = new DisruptorConcurrentMetrics(
                    List.<SupervisedScheduledExecutor<?>>of(bounded, unboundedGroup));
            metrics.bindTo(registry);

            assertThat(gauge(registry, "disruptor.eventloop.accepting",
                    "bounded-metrics", "").value()).isEqualTo(1.0);
            assertThat(gauge(registry, "disruptor.eventloop.worker.registered",
                    "bounded-metrics", "").value()).isEqualTo(1.0);
            assertThat(gauge(registry, "disruptor.eventloop.worker.started",
                    "bounded-metrics", "").value()).isEqualTo(1.0);
            assertThat(gauge(registry, "disruptor.eventloop.worker.alive",
                    "bounded-metrics", "").value()).isEqualTo(1.0);
            assertThat(gauge(registry, "disruptor.eventloop.tasks.outstanding",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.tasks.pending",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.tasks.executing",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.tasks.failed",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.tasks.cancelled",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.tasks.returned",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.tasks.discarded",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.scheduled.pending",
                    "bounded-metrics", "").value()).isZero();
            assertThat(gauge(registry, "disruptor.eventloop.queue.remaining",
                    "bounded-metrics", "").value()).isEqualTo(8.0);

            bounded.submit(() -> { }).get(2, TimeUnit.SECONDS);
            awaitGauge(gauge(registry, "disruptor.eventloop.tasks.completed",
                    "bounded-metrics", ""), 1.0);

            assertThat(gauge(registry, "disruptor.eventloop.queue.segments.allocated",
                    "metrics-group-0", "metrics-group").value()).isGreaterThanOrEqualTo(1.0);
            assertThat(gauge(registry, "disruptor.eventloop.queue.segments.active",
                    "metrics-group-0", "metrics-group").value()).isGreaterThanOrEqualTo(1.0);
            assertThat(registry.find("disruptor.eventloop.queue.segments")
                    .tags("eventloop", "metrics-group-0", "group", "metrics-group")
                    .gauge()).isNull();
            assertThat(registry.find("disruptor.eventloop.queue.remaining")
                    .tags("eventloop", "metrics-group-0", "group", "metrics-group")
                    .gauge()).isNull();
        } finally {
            bounded.shutdownNow();
            unboundedGroup.shutdownNow();
            bounded.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            unboundedGroup.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
    }

    private static Gauge gauge(
            SimpleMeterRegistry registry,
            String metric,
            String eventLoop,
            String group) {
        return registry.get(metric)
                .tags("eventloop", eventLoop, "group", group)
                .gauge();
    }

    private static void awaitGauge(Gauge gauge, double expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (gauge.value() != expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(gauge.value()).isEqualTo(expected);
    }
}
