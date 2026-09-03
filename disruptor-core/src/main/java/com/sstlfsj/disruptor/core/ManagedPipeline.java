package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

/** LMAX Disruptor 与统一受监督生命周期之间的单管道适配。 */
final class ManagedPipeline<E> implements DisruptorPipeline<E> {

    private final String name;
    private final Class<E> eventType;
    private final Disruptor<E> disruptor;
    private final RingBuffer<E> ringBuffer;
    private final SupervisingThreadFactory threadFactory;
    private final WorkerSupervisor supervisor;
    private final PipelineHandle<E> handle = new Handle();
    private final Object lifecycleLock = new Object();
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final CompletionStage<Void> startedView = started.minimalCompletionStage();
    private final CompletableFuture<PipelineSnapshot> terminated = new CompletableFuture<>();
    private final CompletionStage<PipelineSnapshot> terminatedView = terminated.minimalCompletionStage();

    private PublicationGate publicationGate = PublicationGate.NEW;
    private boolean startRequested;
    private int activePublishers;
    private Long drainCursor;

    private ManagedPipeline(
            String name,
            Class<E> eventType,
            Disruptor<E> disruptor,
            SupervisingThreadFactory threadFactory,
            Duration shutdownTimeout) {
        this.name = name;
        this.eventType = eventType;
        this.disruptor = disruptor;
        this.ringBuffer = disruptor.getRingBuffer();
        this.threadFactory = threadFactory;
        this.supervisor = WorkerSupervisor.builder()
                .name(name)
                .shutdownTimeout(shutdownTimeout)
                .shutdownBackend(new LmaxShutdownBackend())
                .build();
        threadFactory.bind(supervisor, this::closePublicationGate);
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
                shutdownTimeout);
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
    public CompletionStage<Void> start() {
        synchronized (lifecycleLock) {
            if (startRequested) {
                return startedView;
            }
            startRequested = true;
            supervisor.markStarting();
            threadFactory.enable();
        }
        try {
            disruptor.start();
        } catch (Throwable failure) {
            closePublicationGate();
            supervisor.fail(failure);
        } finally {
            try {
                supervisor.sealWorkers();
            } catch (Throwable sealFailure) {
                closePublicationGate();
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
        Throwable preStartFailure = null;
        synchronized (lifecycleLock) {
            publicationGate = PublicationGate.CLOSED;
            if (!startRequested) {
                startRequested = true;
                supervisor.markStarting();
                supervisor.sealWorkers();
                preStartFailure = new PipelineStartAbortedException(name);
            }
        }
        supervisor.requestShutdown(mode, deadline);
        if (preStartFailure != null) {
            started.completeExceptionally(preStartFailure);
        }
    }

    @Override
    public CompletionStage<PipelineSnapshot> termination() {
        return terminatedView;
    }

    private void completeStart(Throwable startupFailure) {
        Throwable completionFailure = null;
        boolean failSupervisor = false;
        if (startupFailure != null) {
            completionFailure = unwrap(startupFailure);
            closePublicationGate();
        } else {
            synchronized (lifecycleLock) {
                if (publicationGate == PublicationGate.CLOSED) {
                    completionFailure = startupFailureFromSupervisor();
                } else {
                    try {
                        supervisor.markRunning();
                        publicationGate = PublicationGate.OPEN;
                    } catch (Throwable failure) {
                        publicationGate = PublicationGate.CLOSED;
                        WorkerSnapshot current = supervisor.snapshot();
                        completionFailure = current.failure() == null
                                ? failure
                                : current.failure();
                        failSupervisor = current.failure() == null
                                && current.shutdownMode() == null;
                    }
                }
            }
        }
        if (completionFailure == null) {
            started.complete(null);
            return;
        }
        if (failSupervisor) {
            supervisor.fail(completionFailure);
        }
        started.completeExceptionally(completionFailure);
    }

    private PipelineSnapshot snapshot(WorkerSnapshot worker) {
        long cursor = ringBuffer.getCursor();
        long minimumGating = ringBuffer.getMinimumGatingSequence();
        boolean acceptingPublications;
        synchronized (lifecycleLock) {
            acceptingPublications = publicationGate == PublicationGate.OPEN;
        }
        return PipelineSnapshot.builder()
                .name(name)
                .lifecycle(worker.lifecycle())
                .acceptingPublications(acceptingPublications
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
        synchronized (lifecycleLock) {
            if (publicationGate != PublicationGate.OPEN) {
                return false;
            }
            activePublishers++;
            return true;
        }
    }

    private void enterPublisherOrThrow() {
        if (!enterPublisher()) {
            throw new IllegalStateException("管道 '" + name + "' 当前不接受发布");
        }
    }

    private void exitPublisher() {
        synchronized (lifecycleLock) {
            activePublishers--;
            lifecycleLock.notifyAll();
        }
    }

    private boolean hasActivePublishers() {
        synchronized (lifecycleLock) {
            return activePublishers != 0;
        }
    }

    private void closePublicationGate() {
        synchronized (lifecycleLock) {
            publicationGate = PublicationGate.CLOSED;
            lifecycleLock.notifyAll();
        }
    }

    private Throwable startupFailureFromSupervisor() {
        Throwable failure = supervisor.snapshot().failure();
        return failure == null ? new PipelineStartAbortedException(name) : failure;
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
            closePublicationGate();
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
        private Runnable closePublicationGate;
        private boolean enabled;

        private SupervisingThreadFactory(ThreadFactory delegate) {
            this.delegate = Objects.requireNonNull(delegate, "threadFactory 不能为空");
        }

        private synchronized void bind(
                WorkerSupervisor supervisor,
                Runnable closePublicationGate) {
            if (this.supervisor != null) {
                throw new IllegalStateException("supervisor 已绑定");
            }
            this.supervisor = Objects.requireNonNull(supervisor, "supervisor 不能为空");
            this.closePublicationGate = Objects.requireNonNull(
                    closePublicationGate, "closePublicationGate 不能为空");
        }

        private synchronized void enable() {
            enabled = true;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            WorkerSupervisor currentSupervisor;
            Runnable currentClosePublicationGate;
            synchronized (this) {
                if (!enabled || supervisor == null) {
                    throw new IllegalStateException("线程只能由 DisruptorPipeline.start() 创建");
                }
                currentSupervisor = supervisor;
                currentClosePublicationGate = closePublicationGate;
            }
            Runnable monitoredWorker = () -> {
                try {
                    runnable.run();
                } finally {
                    currentClosePublicationGate.run();
                }
            };
            Runnable supervisedWorker = currentSupervisor.supervise(monitoredWorker);
            Thread thread = Objects.requireNonNull(
                    delegate.newThread(supervisedWorker),
                    "threadFactory 不能返回 null");
            currentSupervisor.register(thread);
            return thread;
        }
    }

    private enum PublicationGate {
        NEW,
        OPEN,
        CLOSED
    }

    private static final class PipelineStartAbortedException extends IllegalStateException {
        private PipelineStartAbortedException(String pipelineName) {
            super("管道在启动前已被关闭：pipeline=" + pipelineName);
        }
    }
}
