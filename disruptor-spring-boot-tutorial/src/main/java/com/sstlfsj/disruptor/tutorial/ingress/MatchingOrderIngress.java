package com.sstlfsj.disruptor.tutorial.ingress;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.sstlfsj.disruptor.autoconfigure.DisruptorLifecycle;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.tutorial.dto.PlaceOrderRequest;
import com.sstlfsj.disruptor.tutorial.pipeline.OrderEvent;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 将并发下单请求串行化到唯一发布线程，再通过原生 RingBuffer 进入 SINGLE 撮合管道。
 * 入口在 Runtime 之后启动、之前停止，确保原生生产者退出后 Runtime 才开始排空。
 */
@Component
public final class MatchingOrderIngress implements SmartLifecycle {

    private static final EventTranslatorOneArg<OrderEvent, OrderCommand> TRANSLATOR =
            (event, sequence, command) -> {
                PlaceOrderRequest request = command.request();
                event.setOrderId(command.orderId());
                event.setSymbol(request.symbol());
                event.setSide(request.side());
                event.setPrice(request.price());
                event.setQuantity(request.quantity());
                event.setTransactTime(command.transactTime());
            };

    private static final long STOP_TIMEOUT_SECONDS = 5L;

    private final RingBuffer<OrderEvent> ringBuffer;
    private final ThreadPoolExecutor publisher;
    private final AtomicLong orderIdGenerator = new AtomicLong();
    private final int phase;
    private volatile boolean running;

    public MatchingOrderIngress(DisruptorRuntime runtime, DisruptorLifecycle runtimeLifecycle) {
        Objects.requireNonNull(runtime, "runtime 不能为空");
        Objects.requireNonNull(runtimeLifecycle, "runtimeLifecycle 不能为空");
        this.ringBuffer = runtime.require("matching", OrderEvent.class).unsafeRingBuffer();
        this.phase = ingressPhase(runtimeLifecycle.getPhase());
        this.publisher = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(ringBuffer.getBufferSize()),
                runnable -> new Thread(runnable, "matching-order-ingress"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 等待唯一发布线程执行一次非阻塞入环；有值表示已经入环，空值表示确定未入环。
     */
    public OptionalLong tryPublish(PlaceOrderRequest request) {
        Objects.requireNonNull(request, "request 不能为空");
        if (!running) {
            return OptionalLong.empty();
        }

        long orderId = orderIdGenerator.incrementAndGet();
        OrderCommand command = new OrderCommand(orderId, request, System.currentTimeMillis());
        Future<Boolean> result;
        try {
            result = publisher.submit(() -> ringBuffer.tryPublishEvent(TRANSLATOR, command));
        } catch (RejectedExecutionException rejected) {
            return OptionalLong.empty();
        }

        try {
            return result.get() ? OptionalLong.of(orderId) : OptionalLong.empty();
        } catch (CancellationException cancelled) {
            return OptionalLong.empty();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待订单入环时被中断", interrupted);
        } catch (ExecutionException failed) {
            throw propagate(failed.getCause());
        }
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (publisher.isShutdown()) {
            throw new IllegalStateException("撮合订单入口已停止，不能重新启动");
        }
        publisher.prestartCoreThread();
        running = true;
    }

    @Override
    public void stop() {
        stopPublisher();
    }

    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        try {
            stopPublisher();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    private synchronized void stopPublisher() {
        if (publisher.isTerminated()) {
            running = false;
            return;
        }
        running = false;
        publisher.shutdown();
        try {
            if (publisher.awaitTermination(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                return;
            }
            cancel(publisher.shutdownNow());
            if (!publisher.awaitTermination(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("撮合订单入口线程未在超时内退出");
            }
        } catch (InterruptedException interrupted) {
            cancel(publisher.shutdownNow());
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待撮合订单入口线程退出时被中断", interrupted);
        }
    }

    private static void cancel(List<Runnable> abandoned) {
        for (Runnable task : abandoned) {
            if (task instanceof Future<?> future) {
                future.cancel(false);
            }
        }
    }

    private static int ingressPhase(int runtimePhase) {
        if (runtimePhase == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "disruptor.lifecycle-phase 必须小于 Integer.MAX_VALUE，才能在 Runtime 前停止订单入口");
        }
        return runtimePhase + 1;
    }

    private static RuntimeException propagate(Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            return runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("订单发布失败", failure);
    }

    private record OrderCommand(long orderId, PlaceOrderRequest request, long transactTime) {
    }
}
