package com.sstlfsj.disruptor;

import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.sstlfsj.disruptor.event.EventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 用 SmartLifecycle 托管 Disruptor 的启停，以便在应用关闭时按阶段有序关闭：
 * start 阶段启动消费线程，stop 阶段先排空 RingBuffer 再停止，超时则强制 halt。
 * phase 越大越先停止，事件总线默认取极小值，保证在上游（请求、MQ 等）停止后才关闭。
 */
public class DisruptorLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DisruptorLifecycle.class);

    private final Disruptor<EventWrapper> disruptor;
    private final Duration shutdownTimeout;
    private final int phase;
    private volatile boolean running = false;

    public DisruptorLifecycle(Disruptor<EventWrapper> disruptor, Duration shutdownTimeout, int phase) {
        this.disruptor = disruptor;
        this.shutdownTimeout = shutdownTimeout;
        this.phase = phase;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        disruptor.start();
        running = true;
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        try {
            disruptor.shutdown(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Disruptor 在 {} 内未排空完成，强制停止，可能丢弃未消费事件", shutdownTimeout, e);
            disruptor.halt();
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
