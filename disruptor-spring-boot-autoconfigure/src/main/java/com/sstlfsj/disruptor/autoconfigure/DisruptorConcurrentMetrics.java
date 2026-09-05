package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.CapacityMode;
import com.sstlfsj.disruptor.concurrent.EventLoopGroupSnapshot;
import com.sstlfsj.disruptor.concurrent.EventLoopSnapshot;
import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.util.List;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

/** 采集 EventLoop 快照，不包装任务提交或执行热路径。 */
public final class DisruptorConcurrentMetrics implements MeterBinder {

    private final List<SupervisedScheduledExecutor<?>> roots;

    public DisruptorConcurrentMetrics(List<SupervisedScheduledExecutor<?>> roots) {
        this.roots = List.copyOf(Objects.requireNonNull(roots, "roots 不能为空"));
        if (this.roots.isEmpty()) {
            throw new IllegalArgumentException("roots 不能为空");
        }
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry 不能为空");
        for (SupervisedScheduledExecutor<?> root : roots) {
            Object snapshot = root.snapshot();
            if (snapshot instanceof EventLoopSnapshot loop) {
                register(
                        registry,
                        () -> (EventLoopSnapshot) root.snapshot(),
                        loop.name(),
                        "",
                        loop.capacityMode());
            } else if (snapshot instanceof EventLoopGroupSnapshot group) {
                for (int childIndex = 0; childIndex < group.children().size(); childIndex++) {
                    int stableIndex = childIndex;
                    EventLoopSnapshot child = group.children().get(stableIndex);
                    register(
                            registry,
                            () -> ((EventLoopGroupSnapshot) root.snapshot())
                                    .children().get(stableIndex),
                            child.name(),
                            group.name(),
                            child.capacityMode());
                }
            } else {
                throw new IllegalStateException("不支持的 concurrent 根快照类型："
                        + snapshot.getClass().getName());
            }
        }
    }

    private static void register(
            MeterRegistry registry,
            SnapshotSource source,
            String eventLoop,
            String group,
            CapacityMode capacityMode) {
        register(registry, "disruptor.eventloop.accepting", source,
                snapshot -> snapshot.acceptingTasks() ? 1.0 : 0.0, eventLoop, group);
        register(registry, "disruptor.eventloop.worker.registered", source,
                snapshot -> snapshot.worker().registeredWorkers(), eventLoop, group);
        register(registry, "disruptor.eventloop.worker.started", source,
                snapshot -> snapshot.worker().startedWorkers(), eventLoop, group);
        register(registry, "disruptor.eventloop.worker.alive", source,
                snapshot -> snapshot.worker().aliveWorkers(), eventLoop, group);
        register(registry, "disruptor.eventloop.tasks.outstanding", source,
                EventLoopSnapshot::outstandingTasks, eventLoop, group);
        register(registry, "disruptor.eventloop.tasks.pending", source,
                EventLoopSnapshot::ingressPendingTasks, eventLoop, group);
        register(registry, "disruptor.eventloop.tasks.executing", source,
                EventLoopSnapshot::executingTasks, eventLoop, group);
        register(registry, "disruptor.eventloop.tasks.completed", source,
                EventLoopSnapshot::completedTasks, eventLoop, group);
        register(registry, "disruptor.eventloop.tasks.failed", source,
                EventLoopSnapshot::failedTasks, eventLoop, group);
        register(registry, "disruptor.eventloop.tasks.cancelled", source,
                EventLoopSnapshot::cancelledTasks, eventLoop, group);
        register(registry, "disruptor.eventloop.tasks.returned", source,
                EventLoopSnapshot::shutdownNowReturnedTasks, eventLoop, group);
        register(registry, "disruptor.eventloop.scheduled.pending", source,
                EventLoopSnapshot::scheduledPendingTasks, eventLoop, group);
        if (capacityMode == CapacityMode.BOUNDED) {
            register(registry, "disruptor.eventloop.queue.remaining", source,
                    snapshot -> snapshot.remainingCapacity().orElseThrow(), eventLoop, group);
        } else {
            register(registry, "disruptor.eventloop.queue.segments", source,
                    EventLoopSnapshot::allocatedQueueSegments, eventLoop, group);
        }
    }

    private static void register(
            MeterRegistry registry,
            String metric,
            SnapshotSource source,
            ToDoubleFunction<EventLoopSnapshot> value,
            String eventLoop,
            String group) {
        Gauge.builder(metric, source, current -> value.applyAsDouble(current.snapshot()))
                .tags("eventloop", eventLoop, "group", group)
                .register(registry);
    }

    @FunctionalInterface
    private interface SnapshotSource {
        EventLoopSnapshot snapshot();
    }
}
