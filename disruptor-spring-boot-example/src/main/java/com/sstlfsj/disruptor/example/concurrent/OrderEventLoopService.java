package com.sstlfsj.disruptor.example.concurrent;

import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.CancellationSource;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoopGroup;
import com.sstlfsj.disruptor.concurrent.EventLoopScheduledFuture;
import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSpec;
import com.sstlfsj.disruptor.concurrent.TaskContext;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** concurrent 模块的显式 owner、上下文、调度、取消和 affinity 用法。 */
@RequiredArgsConstructor
public final class OrderEventLoopService {

    private static final TaskContext.Key<String> ORDER_ID =
            TaskContext.Key.of("order-id", String.class);
    private static final TaskContext.Key<String> TENANT =
            TaskContext.Key.of("tenant", String.class);

    private final DisruptorEventLoop orderEventLoop;
    private final DisruptorEventLoopGroup orderWorkerGroup;

    public EventLoopScheduledFuture<String> submitOrder(String orderId) {
        TaskContext context = TaskContext.empty()
                .with(ORDER_ID, orderId)
                .with(TENANT, "tenant-a");
        return orderEventLoop.schedule(ScheduledTaskSpec.<String>builder()
                .context(context)
                .task(current -> current.get(ORDER_ID) + "@" + current.get(TENANT))
                .build());
    }

    public ScheduledFuture<?> startFixedRate(Runnable action) {
        return orderEventLoop.scheduleAtFixedRate(
                action, 0, 1, TimeUnit.MILLISECONDS);
    }

    public EventLoopScheduledFuture<Void> startDynamicDelay(Runnable action) {
        return orderEventLoop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    action.run();
                    return null;
                })
                .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                .dynamicDelay(lastRun -> Duration.ofMillis(lastRun.executions()))
                .maxExecutions(3)
                .build());
    }

    public CompletionStage<CancellationReason> observeCancellation(CancellationSource source) {
        CompletableFuture<CancellationReason> observed = new CompletableFuture<>();
        source.onCancellation(orderEventLoop, observed::complete);
        return observed;
    }

    public Future<String> affinityWorker(int affinityKey) {
        return orderWorkerGroup.select(affinityKey)
                .submit(() -> Thread.currentThread().getName());
    }
}
