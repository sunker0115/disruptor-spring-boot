package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineHandle;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/** 轮询原生 RingBuffer 状态，不包装发布或消费热路径。 */
public final class DisruptorMetrics implements MeterBinder {

    private final DisruptorRuntime runtime;

    public DisruptorMetrics(DisruptorRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("disruptor.runtime.running", runtime,
                        value -> value.isRunning() ? 1.0 : 0.0)
                .description("Disruptor runtime running state")
                .register(registry);
        for (PipelineHandle<?> handle : runtime.handles()) {
            registerPipelineGauges(registry, handle);
        }
    }

    private static void registerPipelineGauges(MeterRegistry registry, PipelineHandle<?> handle) {
        Gauge.builder("disruptor.pipeline.buffer.size", handle,
                        value -> value.unsafeRingBuffer().getBufferSize())
                .description("Disruptor ring buffer size")
                .tags("pipeline", handle.name(), "event.type", handle.eventType().getName())
                .register(registry);
        Gauge.builder("disruptor.pipeline.remaining.capacity", handle,
                        value -> value.unsafeRingBuffer().remainingCapacity())
                .description("Disruptor ring buffer remaining capacity")
                .tags("pipeline", handle.name(), "event.type", handle.eventType().getName())
                .register(registry);
        Gauge.builder("disruptor.pipeline.backlog", handle, DisruptorMetrics::backlog)
                .description("Approximate Disruptor ring buffer backlog")
                .tags("pipeline", handle.name(), "event.type", handle.eventType().getName())
                .register(registry);
    }

    private static double backlog(PipelineHandle<?> handle) {
        long cursor = handle.unsafeRingBuffer().getCursor();
        long minimumGatingSequence = handle.unsafeRingBuffer().getMinimumGatingSequence();
        return Math.max(0L, cursor - minimumGatingSequence);
    }
}
