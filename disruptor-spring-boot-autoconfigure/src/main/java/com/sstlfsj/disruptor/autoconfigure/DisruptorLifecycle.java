package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.core.DisruptorRuntime;
import org.springframework.context.SmartLifecycle;

import java.util.Objects;

/** 将纯 Java {@link DisruptorRuntime} 接入 Spring 一次性生命周期。 */
public final class DisruptorLifecycle implements SmartLifecycle {

    private final DisruptorRuntime runtime;
    private final int phase;

    public DisruptorLifecycle(DisruptorRuntime runtime, int phase) {
        this.runtime = runtime;
        this.phase = phase;
    }

    @Override
    public void start() {
        runtime.start();
    }

    @Override
    public void stop() {
        runtime.shutdown();
    }

    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        try {
            runtime.shutdown();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return runtime.isRunning();
    }

    @Override
    public int getPhase() {
        return phase;
    }
}
