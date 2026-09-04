package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.concurrent.internal.EventLoopKernel;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** 两种公开 EventLoop 门面共享的纯委派实现。 */
abstract class AbstractEventLoop extends AbstractExecutorService implements EventLoop {

    private EventLoopKernel kernel;

    final void initializeKernel(EventLoopKernel kernel) {
        if (this.kernel != null) {
            throw new IllegalStateException("EventLoopKernel 只能初始化一次");
        }
        this.kernel = Objects.requireNonNull(kernel, "kernel 不能为空");
    }

    @Override
    public final String name() {
        return kernel().name();
    }

    @Override
    public final CompletionStage<Void> start() {
        return kernel().start();
    }

    @Override
    public final void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        kernel().requestShutdown(mode, deadline);
    }

    @Override
    public final CompletionStage<EventLoopSnapshot> termination() {
        return kernel().termination();
    }

    @Override
    public final EventLoopSnapshot snapshot() {
        return kernel().snapshot();
    }

    @Override
    public final boolean inEventLoop() {
        return kernel().inEventLoop();
    }

    @Override
    public EventLoopGroup parent() {
        return null;
    }

    @Override
    public final boolean tryExecute(Runnable command) {
        return kernel().tryExecute(command);
    }

    @Override
    public final <V> EventLoopScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec) {
        return kernel().schedule(spec);
    }

    @Override
    public final void execute(Runnable command) {
        kernel().execute(command);
    }

    @Override
    public final Future<?> submit(Runnable task) {
        return kernel().submit(task);
    }

    @Override
    public final <T> Future<T> submit(Runnable task, T result) {
        return kernel().submit(task, result);
    }

    @Override
    public final <T> Future<T> submit(Callable<T> task) {
        return kernel().submit(task);
    }

    @Override
    public final ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return kernel().schedule(command, delay, unit);
    }

    @Override
    public final <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return kernel().schedule(callable, delay, unit);
    }

    @Override
    public final ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit) {
        return kernel().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public final ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command,
            long initialDelay,
            long delay,
            TimeUnit unit) {
        return kernel().scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public final void shutdown() {
        kernel().shutdown();
    }

    @Override
    public final List<Runnable> shutdownNow() {
        return kernel().shutdownNow();
    }

    @Override
    public final boolean isShutdown() {
        return kernel().isShutdown();
    }

    @Override
    public final boolean isTerminated() {
        return kernel().isTerminated();
    }

    @Override
    public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return kernel().awaitTermination(timeout, unit);
    }

    @Override
    public final <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        rejectBlockingInvoke(tasks);
        return super.invokeAll(tasks);
    }

    @Override
    public final <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit) throws InterruptedException {
        rejectBlockingInvoke(tasks);
        return super.invokeAll(tasks, timeout, unit);
    }

    @Override
    public final <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        rejectBlockingInvoke(tasks);
        return super.invokeAny(tasks);
    }

    @Override
    public final <T> T invokeAny(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        rejectBlockingInvoke(tasks);
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public final void close() {
        kernel().close();
    }

    private EventLoopKernel kernel() {
        EventLoopKernel current = kernel;
        if (current == null) {
            throw new IllegalStateException("EventLoopKernel 尚未初始化");
        }
        return current;
    }

    private void rejectBlockingInvoke(Collection<?> tasks) {
        Objects.requireNonNull(tasks, "tasks 不能为空");
        if (!tasks.isEmpty() && inEventLoop()) {
            throw new BlockingOperationException("不能在所属 EventLoop 线程执行阻塞式批量调用");
        }
    }
}
