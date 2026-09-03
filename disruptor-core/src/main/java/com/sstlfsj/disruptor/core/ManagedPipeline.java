package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** LMAX Disruptor 与统一受监督生命周期之间的单管道适配。 */
final class ManagedPipeline<E> implements DisruptorPipeline<E> {

    private final String name;
    private final Class<E> eventType;
    private final Disruptor<E> disruptor;
    private final RingBuffer<E> ringBuffer;
    private final SupervisingThreadFactory threadFactory;
    private final WorkerSupervisor supervisor;
    private final PipelineHandle<E> handle = new Handle();
    private final boolean singleProducer;
    private final AtomicInteger activePublishers;
    private final AtomicBoolean acceptingPublications = new AtomicBoolean();
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final CompletionStage<Void> startedView = started.minimalCompletionStage();
    private final CompletableFuture<PipelineSnapshot> terminated = new CompletableFuture<>();
    private final CompletionStage<PipelineSnapshot> terminatedView = terminated.minimalCompletionStage();

    private boolean startRequested;
    private volatile boolean singlePublisherActive;
    private Long drainCursor;

    private ManagedPipeline(
            String name,
            Class<E> eventType,
            Disruptor<E> disruptor,
            SupervisingThreadFactory threadFactory,
            ProducerType producerType,
            Duration shutdownTimeout) {
        this.name = name;
        this.eventType = eventType;
        this.disruptor = disruptor;
        this.ringBuffer = disruptor.getRingBuffer();
        this.threadFactory = threadFactory;
        this.singleProducer = producerType == ProducerType.SINGLE;
        this.activePublishers = singleProducer ? null : new AtomicInteger();
        this.supervisor = WorkerSupervisor.builder()
                .name(name)
                .shutdownTimeout(shutdownTimeout)
                .shutdownBackend(new LmaxShutdownBackend())
                .build();
        threadFactory.bind(supervisor, acceptingPublications);
        supervisor.termination().whenComplete((workerSnapshot, failure) -> {
            if (failure != null) {
                terminated.completeExceptionally(unwrap(failure));
            } else {
                terminated.complete(snapshot(workerSnapshot));
            }
        });
    }

    static <E> ManagedPipeline<E> build(
            PipelineSpec<E> spec,
            ResolvedPipelineSettings<E> settings,
            Duration shutdownTimeout) {
        Objects.requireNonNull(spec, "spec 不能为空");
        Objects.requireNonNull(settings, "settings 不能为空");
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout 不能为空");
        SupervisingThreadFactory threadFactory = new SupervisingThreadFactory(settings.threadFactory());
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
                    + "' 的 topology 不得调用 start()，生命周期必须由 DisruptorPipeline 托管");
        }
        return new ManagedPipeline<>(spec.name(), spec.eventType(), disruptor, threadFactory,
                settings.producerType(), shutdownTimeout);
    }

    @Override
    public PipelineHandle<E> handle() {
        return handle;
    }

    @Override
    public PipelineSnapshot snapshot() {
        return snapshot(supervisor.snapshot());
    }

    @Override
    public synchronized CompletionStage<Void> start() {
        if (startRequested) {
            return startedView;
        }
        startRequested = true;
        supervisor.markStarting();
        threadFactory.enable();
        try {
            disruptor.start();
        } catch (Throwable failure) {
            acceptingPublications.set(false);
            supervisor.fail(failure);
        } finally {
            try {
                supervisor.sealWorkers();
            } catch (Throwable sealFailure) {
                acceptingPublications.set(false);
                supervisor.fail(sealFailure);
            }
        }
        supervisor.workersStarted().whenComplete((ignored, failure) -> completeStart(failure));
        return startedView;
    }

    @Override
    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        Objects.requireNonNull(mode, "mode 不能为空");
        Objects.requireNonNull(deadline, "deadline 不能为空");
        acceptingPublications.set(false);
        supervisor.requestShutdown(mode, deadline);
    }

    @Override
    public CompletionStage<PipelineSnapshot> termination() {
        return terminatedView;
    }

    private void completeStart(Throwable startupFailure) {
        if (startupFailure != null) {
            started.completeExceptionally(unwrap(startupFailure));
            return;
        }
        try {
            supervisor.markRunning();
            acceptingPublications.set(true);
            WorkerSnapshot current = supervisor.snapshot();
            if (current.lifecycle() != PipelineLifecycle.RUNNING) {
                acceptingPublications.set(false);
                throw current.failure() == null
                        ? new IllegalStateException("管道 '" + name + "' 未能保持 RUNNING")
                        : current.failure();
            }
            started.complete(null);
        } catch (Throwable failure) {
            acceptingPublications.set(false);
            supervisor.fail(failure);
            Throwable firstFailure = supervisor.snapshot().failure();
            started.completeExceptionally(firstFailure == null ? failure : firstFailure);
        }
    }

    private PipelineSnapshot snapshot(WorkerSnapshot worker) {
        long cursor = ringBuffer.getCursor();
        long minimumGating = ringBuffer.getMinimumGatingSequence();
        return PipelineSnapshot.builder()
                .name(name)
                .lifecycle(worker.lifecycle())
                .acceptingPublications(acceptingPublications.get()
                        && worker.lifecycle() == PipelineLifecycle.RUNNING)
                .registrationSealed(worker.registrationSealed())
                .expectedConsumers(worker.expectedWorkers())
                .createdConsumers(worker.registeredWorkers())
                .startedConsumers(worker.startedWorkers())
                .aliveConsumers(worker.aliveWorkers())
                .bufferSize(ringBuffer.getBufferSize())
                .backlog(Math.max(0L, cursor - minimumGating))
                .failure(worker.failure())
                .shutdownMode(worker.shutdownMode())
                .reachedRunning(worker.reachedRunning())
                .drainCommitted(worker.drainCommitted())
                .gracefulStopApplied(worker.gracefulStopApplied())
                .build();
    }

    private boolean enterPublisher() {
        if (!acceptingPublications.get()) {
            return false;
        }
        if (singleProducer) {
            singlePublisherActive = true;
            if (acceptingPublications.get()) {
                return true;
            }
            singlePublisherActive = false;
            return false;
        }
        activePublishers.incrementAndGet();
        if (acceptingPublications.get()) {
            return true;
        }
        activePublishers.decrementAndGet();
        return false;
    }

    private void enterPublisherOrThrow() {
        if (!enterPublisher()) {
            throw new IllegalStateException("管道 '" + name + "' 当前不接受发布");
        }
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

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private final class LmaxShutdownBackend implements ShutdownBackend {

        @Override
        public void beginQuiesce() {
            acceptingPublications.set(false);
        }

        @Override
        public boolean isDrained() {
            if (hasActivePublishers()) {
                return false;
            }
            if (drainCursor == null) {
                drainCursor = ringBuffer.getCursor();
            }
            // LMAX DSL 会在追加下游时移除上游 gating sequence；公开 API 返回的最小值
            // 因而覆盖全部真实叶子，达到固定 cursor 等价于每个叶子都已排空。
            return ringBuffer.getMinimumGatingSequence() >= drainCursor;
        }

        @Override
        public void stop(ShutdownMode mode) {
            disruptor.halt();
        }
    }

    private final class Handle implements PipelineHandle<E> {

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<E> eventType() {
            return eventType;
        }

        @Override
        public PipelineSnapshot snapshot() {
            return ManagedPipeline.this.snapshot();
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
        public <A, B> void publishEvent(
                EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
            Objects.requireNonNull(translator, "translator 不能为空");
            enterPublisherOrThrow();
            try {
                ringBuffer.publishEvent(translator, arg0, arg1);
            } finally {
                exitPublisher();
            }
        }

        @Override
        public <A, B> boolean tryPublishEvent(
                EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
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
        public <A, B, C> void publishEvent(
                EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2) {
            Objects.requireNonNull(translator, "translator 不能为空");
            enterPublisherOrThrow();
            try {
                ringBuffer.publishEvent(translator, arg0, arg1, arg2);
            } finally {
                exitPublisher();
            }
        }

        @Override
        public <A, B, C> boolean tryPublishEvent(
                EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2) {
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
    }

    private static final class SupervisingThreadFactory implements ThreadFactory {

        private final ThreadFactory delegate;
        private WorkerSupervisor supervisor;
        private AtomicBoolean acceptingPublications;
        private boolean enabled;

        private SupervisingThreadFactory(ThreadFactory delegate) {
            this.delegate = Objects.requireNonNull(delegate, "threadFactory 不能为空");
        }

        private synchronized void bind(
                WorkerSupervisor supervisor,
                AtomicBoolean acceptingPublications) {
            if (this.supervisor != null) {
                throw new IllegalStateException("supervisor 已绑定");
            }
            this.supervisor = Objects.requireNonNull(supervisor, "supervisor 不能为空");
            this.acceptingPublications = Objects.requireNonNull(
                    acceptingPublications, "acceptingPublications 不能为空");
        }

        private synchronized void enable() {
            enabled = true;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            WorkerSupervisor currentSupervisor;
            AtomicBoolean currentAcceptance;
            synchronized (this) {
                if (!enabled || supervisor == null) {
                    throw new IllegalStateException("线程只能由 DisruptorPipeline.start() 创建");
                }
                currentSupervisor = supervisor;
                currentAcceptance = acceptingPublications;
            }
            Runnable monitoredWorker = () -> {
                try {
                    runnable.run();
                } catch (Throwable failure) {
                    WorkerSnapshot snapshot = currentSupervisor.snapshot();
                    if (!isControlledInterruption(failure, snapshot)) {
                        sneakyThrow(failure);
                    }
                } finally {
                    currentAcceptance.set(false);
                }
            };
            Thread thread = Objects.requireNonNull(
                    delegate.newThread(currentSupervisor.supervise(monitoredWorker)),
                    "threadFactory 不能返回 null");
            currentSupervisor.register(thread);
            return thread;
        }

        private static boolean isControlledInterruption(
                Throwable failure,
                WorkerSnapshot snapshot) {
            if (snapshot.lifecycle() != PipelineLifecycle.STOPPING
                    || snapshot.shutdownMode() != ShutdownMode.IMMEDIATE) {
                return false;
            }
            Throwable current = failure;
            while (current != null) {
                if (current instanceof InterruptedException) {
                    return true;
                }
                current = current.getCause();
            }
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }
}
