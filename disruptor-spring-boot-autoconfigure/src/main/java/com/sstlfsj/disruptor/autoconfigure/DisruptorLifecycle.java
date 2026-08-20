package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.sstlfsj.disruptor.core.DisruptorPipeline;
import com.sstlfsj.disruptor.core.Pipelines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 用 SmartLifecycle 托管所有管道 Disruptor 的启停：start 阶段启动全部管道的消费线程，
 * stop 阶段逐个先排空 RingBuffer 再停止，超时则强制 halt。phase 取极小值，保证在上游
 * （请求、MQ 等）停止后才关闭。
 */
public class DisruptorLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DisruptorLifecycle.class);

    private final Pipelines pipelines;
    private final Duration shutdownTimeout;
    private final int phase;
    private volatile boolean running = false;

    public DisruptorLifecycle(Pipelines pipelines, Duration shutdownTimeout, int phase) {
        this.pipelines = pipelines;
        this.shutdownTimeout = shutdownTimeout;
        this.phase = phase;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        int count = 0;
        for (DisruptorPipeline<?> pipeline : pipelines.all()) {
            pipeline.disruptor().start();
            log.info("已启动管道 [{}]（事件类型 {}）", pipeline.name(), pipeline.eventType().getSimpleName());
            count++;
        }
        running = true;
        log.info("Disruptor 事件总线已启动，共 {} 条管道", count);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        log.info("Disruptor 事件总线开始关闭，共 {} 条管道", pipelines.all().size());
        for (DisruptorPipeline<?> pipeline : pipelines.all()) {
            Disruptor<?> disruptor = pipeline.disruptor();
            try {
                disruptor.shutdown(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
                log.info("管道 [{}] 已排空并关闭", pipeline.name());
            } catch (TimeoutException e) {
                log.warn("管道 [{}] 在 {} 内未排空完成，强制停止，可能丢弃未消费事件",
                        pipeline.name(), shutdownTimeout, e);
                disruptor.halt();
            }
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
}
