package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.concurrent.internal.EventLoopKernel;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** 默认的有界、严格单线程 EventLoop。 */
public final class DisruptorEventLoop extends AbstractExecutorService implements EventLoop {

    private final EventLoopKernel kernel;

    DisruptorEventLoop(
            String name,
            int capacity,
            ThreadFactory threadFactory,
            Duration shutdownTimeout,
            NanoClock clock,
            int maxCommandBatchSize,
            int maxTimerBatchSize,
            TaskExceptionHandler taskExceptionHandler,
            List<EventLoopModule> modules) {
        kernel = EventLoopKernel.bounded(
                this,
                name,
                capacity,
                threadFactory,
                shutdownTimeout,
                clock,
                maxCommandBatchSize,
                maxTimerBatchSize,
                taskExceptionHandler,
                modules);
    }

    @Override
    public String name() {
        return kernel.name();
    }

    @Override
    public CompletionStage<Void> start() {
        return kernel.start();
    }

    @Override
    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        kernel.requestShutdown(mode, deadline);
    }

    @Override
    public CompletionStage<EventLoopSnapshot> termination() {
        return kernel.termination();
    }

    @Override
    public EventLoopSnapshot snapshot() {
        return kernel.snapshot();
    }

    @Override
    public boolean inEventLoop() {
        return kernel.inEventLoop();
    }

    @Override
    public EventLoopGroup parent() {
        return null;
    }

    @Override
    public boolean tryExecute(Runnable command) {
        return kernel.tryExecute(command);
    }

    @Override
    public <V> EventLoopScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec) {
        return kernel.schedule(spec);
    }

    @Override
    public void execute(Runnable command) {
        kernel.execute(command);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return kernel.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return kernel.submit(task, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return kernel.submit(task);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return kernel.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return kernel.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit) {
        return kernel.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command,
            long initialDelay,
            long delay,
            TimeUnit unit) {
        return kernel.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        kernel.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return kernel.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return kernel.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return kernel.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return kernel.awaitTermination(timeout, unit);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        rejectBlockingInvoke(tasks);
        return super.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit) throws InterruptedException {
        rejectBlockingInvoke(tasks);
        return super.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        rejectBlockingInvoke(tasks);
        return super.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        rejectBlockingInvoke(tasks);
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void close() {
        kernel.close();
    }

    private void rejectBlockingInvoke(Collection<?> tasks) {
        Objects.requireNonNull(tasks, "tasks 不能为空");
        if (!tasks.isEmpty() && inEventLoop()) {
            throw new BlockingOperationException("不能在所属 EventLoop 线程执行阻塞式批量调用");
        }
    }
}
