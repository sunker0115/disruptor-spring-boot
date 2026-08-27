package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/** 单条管道的受管发布、排空和线程终止边界。 */
final class ManagedPipeline<E> implements PipelineHandle<E> {

    private static final long PARK_NANOS = 100_000L;

    private final String name;
    private final Class<E> eventType;
    private final Disruptor<E> disruptor;
    private final RingBuffer<E> ringBuffer;
    private final TrackingThreadFactory threadFactory;
    private final boolean singleProducer;
    private final AtomicInteger activePublishers;
    private volatile boolean acceptingPublications;
    private volatile boolean singlePublisherActive;

    private ManagedPipeline(String name, Class<E> eventType, Disruptor<E> disruptor,
                            TrackingThreadFactory threadFactory, ProducerType producerType) {
        this.name = name;
        this.eventType = eventType;
        this.disruptor = disruptor;
        this.ringBuffer = disruptor.getRingBuffer();
        this.threadFactory = threadFactory;
        this.singleProducer = producerType == ProducerType.SINGLE;
        this.activePublishers = singleProducer ? null : new AtomicInteger();
    }

    static <E> ManagedPipeline<E> build(PipelineSpec<E> spec, ResolvedPipelineSettings<E> settings) {
        TrackingThreadFactory threadFactory = new TrackingThreadFactory(settings.threadFactory());
        Disruptor<E> disruptor = new Disruptor<>(
                spec.eventFactory(),
                settings.bufferSize(),
                threadFactory,
                settings.producerType(),
                settings.waitStrategy());
        disruptor.setDefaultExceptionHandler(settings.exceptionHandler());
        try {
            spec.topology().configure(disruptor);
        } catch (RuntimeException | Error failure) {
            if (disruptor.hasStarted()) {
                disruptor.halt();
            }
            throw new IllegalStateException("配置 Disruptor 管道 '" + spec.name() + "' 失败", failure);
        }
        if (disruptor.hasStarted()) {
            disruptor.halt();
            throw new IllegalStateException("管道 '" + spec.name()
                    + "' 的 topology 不得调用 start()，生命周期必须由 DisruptorRuntime 托管");
        }
        return new ManagedPipeline<>(spec.name(), spec.eventType(), disruptor, threadFactory,
                settings.producerType());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Class<E> eventType() {
        return eventType;
    }

    @Override
    public void publishEvent(EventTranslator<E> translator) {
        Objects.requireNonNull(translator, "translator 不能为空");
        enterPublisherOrThrow();
        try {
            ringBuffer.publishEvent(translator);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public boolean tryPublishEvent(EventTranslator<E> translator) {
        Objects.requireNonNull(translator, "translator 不能为空");
        if (!enterPublisher()) {
            return false;
        }
        try {
            return ringBuffer.tryPublishEvent(translator);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public <A> void publishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {
        Objects.requireNonNull(translator, "translator 不能为空");
        enterPublisherOrThrow();
        try {
            ringBuffer.publishEvent(translator, arg0);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public <A> boolean tryPublishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {
        Objects.requireNonNull(translator, "translator 不能为空");
        if (!enterPublisher()) {
            return false;
        }
        try {
            return ringBuffer.tryPublishEvent(translator, arg0);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public <A, B> void publishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
        Objects.requireNonNull(translator, "translator 不能为空");
        enterPublisherOrThrow();
        try {
            ringBuffer.publishEvent(translator, arg0, arg1);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public <A, B> boolean tryPublishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
        Objects.requireNonNull(translator, "translator 不能为空");
        if (!enterPublisher()) {
            return false;
        }
        try {
            return ringBuffer.tryPublishEvent(translator, arg0, arg1);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public <A, B, C> void publishEvent(EventTranslatorThreeArg<E, A, B, C> translator,
                                       A arg0, B arg1, C arg2) {
        Objects.requireNonNull(translator, "translator 不能为空");
        enterPublisherOrThrow();
        try {
            ringBuffer.publishEvent(translator, arg0, arg1, arg2);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public <A, B, C> boolean tryPublishEvent(EventTranslatorThreeArg<E, A, B, C> translator,
                                            A arg0, B arg1, C arg2) {
        Objects.requireNonNull(translator, "translator 不能为空");
        if (!enterPublisher()) {
            return false;
        }
        try {
            return ringBuffer.tryPublishEvent(translator, arg0, arg1, arg2);
        } finally {
            exitPublisher();
        }
    }

    @Override
    public RingBuffer<E> unsafeRingBuffer() {
        return ringBuffer;
    }

    void start() {
        disruptor.start();
        acceptingPublications = true;
    }

    void quiesce() {
        acceptingPublications = false;
    }

    StopResult shutdown(long deadlineNanos) {
        quiesce();
        if (!awaitPublishers(deadlineNanos)) {
            return forceStop(deadlineNanos, "等待在途发布结束超时");
        }

        long targetSequence = ringBuffer.getCursor();
        if (!awaitSequence(targetSequence, deadlineNanos)) {
            return forceStop(deadlineNanos,
                    "等待事件排空超时，目标序列=" + targetSequence
                            + "，最小消费序列=" + ringBuffer.getMinimumGatingSequence());
        }

        Throwable haltFailure = haltProcessors();
        boolean terminated = threadFactory.awaitTermination(deadlineNanos);
        if (!terminated) {
            threadFactory.interruptAlive();
        }
        if (haltFailure != null) {
            return StopResult.failed("停止消费者失败", haltFailure);
        }
        if (!terminated) {
            return StopResult.failed("等待消费线程退出超时", null);
        }
        return StopResult.completed();
    }

    StopResult haltNow(long deadlineNanos) {
        quiesce();
        return forceStop(deadlineNanos, null);
    }

    private StopResult forceStop(long deadlineNanos, String reason) {
        Throwable haltFailure = haltProcessors();
        threadFactory.interruptAlive();
        boolean terminated = threadFactory.awaitTermination(deadlineNanos);
        if (haltFailure != null) {
            return StopResult.failed(reason == null ? "强制停止消费者失败" : reason, haltFailure);
        }
        if (!terminated) {
            return StopResult.failed(reason == null ? "等待消费线程退出超时" : reason, null);
        }
        return reason == null ? StopResult.forced() : StopResult.failed(reason, null);
    }

    private Throwable haltProcessors() {
        try {
            disruptor.halt();
            return null;
        } catch (RuntimeException | Error failure) {
            return failure;
        }
    }

    private boolean awaitPublishers(long deadlineNanos) {
        while (hasActivePublishers()) {
            if (!parkUntil(deadlineNanos)) {
                return false;
            }
        }
        return true;
    }

    private boolean awaitSequence(long targetSequence, long deadlineNanos) {
        while (ringBuffer.getMinimumGatingSequence() < targetSequence) {
            if (!parkUntil(deadlineNanos)) {
                return false;
            }
        }
        return true;
    }

    private static boolean parkUntil(long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0L || Thread.currentThread().isInterrupted()) {
            return false;
        }
        LockSupport.parkNanos(Math.min(PARK_NANOS, remaining));
        return !Thread.currentThread().isInterrupted();
    }

    private void enterPublisherOrThrow() {
        if (!enterPublisher()) {
            throw new IllegalStateException("管道 '" + name + "' 当前不接受发布");
        }
    }

    private boolean enterPublisher() {
        if (!acceptingPublications) {
            return false;
        }
        if (singleProducer) {
            // SINGLE 保证至多一个发布者；两次 volatile 读取与 active 写入形成关闭握手，
            // 避免发布者通过首次检查后，shutdown 在其真正发布前捕获到过早的游标。
            singlePublisherActive = true;
            if (acceptingPublications) {
                return true;
            }
            singlePublisherActive = false;
            return false;
        }
        activePublishers.incrementAndGet();
        if (acceptingPublications) {
            return true;
        }
        activePublishers.decrementAndGet();
        return false;
    }

    private void exitPublisher() {
        if (singleProducer) {
            singlePublisherActive = false;
        } else {
            activePublishers.decrementAndGet();
        }
    }

    private boolean hasActivePublishers() {
        return singleProducer ? singlePublisherActive : activePublishers.get() != 0;
    }

    record StopResult(StopStatus status, String message, Throwable cause) {

        boolean isGraceful() {
            return status == StopStatus.GRACEFUL;
        }

        boolean isSuccessful() {
            return status != StopStatus.FAILED;
        }

        private static StopResult completed() {
            return new StopResult(StopStatus.GRACEFUL, null, null);
        }

        private static StopResult forced() {
            return new StopResult(StopStatus.FORCED, null, null);
        }

        private static StopResult failed(String message, Throwable cause) {
            return new StopResult(StopStatus.FAILED, message, cause);
        }
    }

    private enum StopStatus {
        GRACEFUL,
        FORCED,
        FAILED
    }

    private static final class TrackingThreadFactory implements ThreadFactory {

        private final ThreadFactory delegate;
        private final List<Thread> threads = new ArrayList<>();

        private TrackingThreadFactory(ThreadFactory delegate) {
            this.delegate = Objects.requireNonNull(delegate, "threadFactory 不能为空");
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Objects.requireNonNull(delegate.newThread(runnable),
                    "threadFactory 不能返回 null");
            synchronized (threads) {
                threads.add(thread);
            }
            return thread;
        }

        private void interruptAlive() {
            for (Thread thread : snapshot()) {
                if (thread.isAlive() && thread != Thread.currentThread()) {
                    thread.interrupt();
                }
            }
        }

        private boolean awaitTermination(long deadlineNanos) {
            for (Thread thread : snapshot()) {
                if (thread == Thread.currentThread()) {
                    return false;
                }
                while (thread.isAlive()) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0L) {
                        return false;
                    }
                    try {
                        long millis = remaining / 1_000_000L;
                        int nanos = (int) (remaining % 1_000_000L);
                        thread.join(millis, nanos);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
            return true;
        }

        private List<Thread> snapshot() {
            synchronized (threads) {
                return List.copyOf(threads);
            }
        }
    }
}
