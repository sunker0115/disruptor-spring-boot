package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 冻结 module 顺序并保证部分启动回滚的生命周期执行器。 */
final class ModuleLifecycle {

    private final List<EventLoopModule> modules;
    private final List<EventLoopModule> started;
    private boolean startAttempted;
    private boolean stopped;

    ModuleLifecycle(List<? extends EventLoopModule> modules) {
        Objects.requireNonNull(modules, "modules 不能为空");
        this.modules = List.copyOf(modules);
        this.started = new ArrayList<>(modules.size());
    }

    void start(EventLoop loop) throws Exception {
        if (startAttempted) {
            throw new IllegalStateException("module 启动只能执行一次");
        }
        startAttempted = true;
        for (EventLoopModule module : modules) {
            module.onStart(loop);
            started.add(module);
        }
    }

    void update(EventLoop loop, long nowNanos) throws Exception {
        if (!startAttempted || stopped) {
            throw new IllegalStateException("module 未处于可更新状态");
        }
        for (EventLoopModule module : started) {
            module.onUpdate(loop, nowNanos);
        }
    }

    Throwable stop(EventLoop loop, Throwable primaryFailure) {
        if (stopped) {
            return primaryFailure;
        }
        stopped = true;
        Throwable result = primaryFailure;
        for (int index = started.size() - 1; index >= 0; index--) {
            try {
                started.get(index).onStop(loop);
            } catch (Throwable stopFailure) {
                if (result == null) {
                    result = stopFailure;
                } else if (result != stopFailure) {
                    result.addSuppressed(stopFailure);
                }
            }
        }
        return result;
    }

    int startedCount() {
        return started.size();
    }
}
