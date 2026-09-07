package com.sstlfsj.disruptor.example.nospring;

import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.CancellationSource;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopScheduledFuture;
import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSpec;
import com.sstlfsj.disruptor.concurrent.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 纯 Java 的 concurrent 用法；默认选择有界队列。
 * 无界队列只适合可控的内部短突发或控制流，不能替代上游限流。
 */
public final class PureJavaConcurrentExample {

    private static final Logger log = LoggerFactory.getLogger(PureJavaConcurrentExample.class);
    private static final TaskContext.Key<String> REQUEST_ID =
            TaskContext.Key.of("request-id", String.class);

    private PureJavaConcurrentExample() {
    }

    public static void main(String[] args) throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder.bounded("pure-java-concurrent", 8)
                .shutdownTimeout(Duration.ofSeconds(2))
                .build();
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        try {
            TaskContext context = TaskContext.empty().with(REQUEST_ID, "request-42");
            EventLoopScheduledFuture<String> oneShot = loop.schedule(
                    ScheduledTaskSpec.<String>builder()
                            .context(context)
                            .task(current -> current.get(REQUEST_ID))
                            .build());
            log.info("[pure-concurrent] one-shot context={}",
                    oneShot.get(2, TimeUnit.SECONDS));

            requireAccepted(loop.tryExecute(() ->
                    log.info("[pure-concurrent] tryExecute 已接收")));

            EventLoopScheduledFuture<Void> dynamic = loop.schedule(
                    ScheduledTaskSpec.<Void>builder()
                            .task(current -> {
                                log.info("[pure-concurrent] dynamic schedule 执行");
                                return null;
                            })
                            .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                            .dynamicDelay(lastRun -> Duration.ofMillis(lastRun.executions()))
                            .maxExecutions(2)
                            .build());
            awaitDone(dynamic);

            CancellationSource cancellation = new CancellationSource();
            CountDownLatch cancelled = new CountDownLatch(1);
            cancellation.onCancellation(loop, reason -> {
                log.info("[pure-concurrent] cancellation={}", reason.code());
                cancelled.countDown();
            });
            cancellation.cancel(CancellationReason.of("demo-complete"));
            if (!cancelled.await(2, TimeUnit.SECONDS)) {
                throw new IllegalStateException("取消监听器未完成");
            }
            runInternalBurstExample();
        } finally {
            loop.shutdown();
            if (!loop.awaitTermination(2, TimeUnit.SECONDS)) {
                loop.shutdownNow();
                if (!loop.awaitTermination(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("EventLoop 未在超时内终止");
                }
            }
        }
        log.info("[pure-concurrent] 完成");
    }

    private static void runInternalBurstExample() throws Exception {
        EventLoop burst = EventLoopBuilder.unbounded(
                        "pure-java-internal-burst", 4)
                .shutdownTimeout(Duration.ofSeconds(2))
                .build();
        CountDownLatch completed = new CountDownLatch(2);
        burst.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        try {
            burst.execute(completed::countDown);
            burst.execute(completed::countDown);
            if (!completed.await(2, TimeUnit.SECONDS)) {
                throw new IllegalStateException("内部突发任务未完成");
            }
            log.info("[pure-concurrent] unbounded 内部短突发已完成");
        } finally {
            burst.shutdown();
            if (!burst.awaitTermination(2, TimeUnit.SECONDS)) {
                burst.shutdownNow();
                if (!burst.awaitTermination(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("内部短突发 EventLoop 未终止");
                }
            }
        }
    }

    private static void requireAccepted(boolean accepted) {
        if (!accepted) {
            throw new RejectedExecutionException("有界 EventLoop 已满，调用方应立即限流或降级");
        }
    }

    private static void awaitDone(EventLoopScheduledFuture<?> future) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (!future.isDone() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        if (!future.isDone()) {
            throw new IllegalStateException("动态调度未在超时内完成");
        }
    }
}
