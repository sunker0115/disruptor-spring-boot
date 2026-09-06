package com.sstlfsj.disruptor.example.concurrent;

import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.CancellationSource;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** 复用 Spring 管理的 EventLoop 与 Group，演示提交、亲和性及取消。 */
@Component
@Order(6)
@RequiredArgsConstructor
public final class ConcurrentDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentDemoRunner.class);

    private final OrderEventLoopService service;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo6 concurrent 显式上下文、affinity 与取消 ====");
        log.info("[concurrent] context result={}",
                service.submitOrder("order-42").get(2, TimeUnit.SECONDS));

        String firstWorker = service.affinityWorker(7).get(2, TimeUnit.SECONDS);
        String sameWorker = service.affinityWorker(7).get(2, TimeUnit.SECONDS);
        log.info("[concurrent] affinity key=7 first={}, same={}", firstWorker, sameWorker);

        AtomicInteger fixedRateRuns = new AtomicInteger();
        ScheduledFuture<?> fixedRate = service.startFixedRate(fixedRateRuns::incrementAndGet);
        awaitAtLeast(fixedRateRuns, 2);
        fixedRate.cancel(false);
        log.info("[concurrent] fixed-rate 已取消，runs={}", fixedRateRuns.get());

        CancellationSource cancellation = new CancellationSource();
        var observed = service.observeCancellation(cancellation);
        CancellationReason reason = CancellationReason.of("demo-complete");
        cancellation.cancel(reason);
        log.info("[concurrent] cancellation={}",
                observed.toCompletableFuture().get(2, TimeUnit.SECONDS).code());

        results.markDone("concurrent");
        log.info("==== demo6 完成（EventLoop 由 Spring 生命周期关闭） ====");
    }

    private static void awaitAtLeast(AtomicInteger runs, int expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (runs.get() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        if (runs.get() < expected) {
            throw new IllegalStateException("fixed-rate 未在超时内执行");
        }
    }
}
