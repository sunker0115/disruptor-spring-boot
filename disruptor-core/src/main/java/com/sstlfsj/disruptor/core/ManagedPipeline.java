package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/** LMAX Disruptor 与统一受监督生命周期之间的单管道适配。 */
final class ManagedPipeline<E> implements DisruptorPipeline<E> {

    private static final long PUBLICATION_RETRY_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

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
    private final Set<Thread> publicationWaiters =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private PublicationGate publicationGate = PublicationGate.NEW;
    private boolean workerFailurePending;
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
        threadFactory.bind(supervisor, this::closePublicationGateAfterWorkerExit);
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
            closePublicationGateLocked();
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
                        closePublicationGateLocked();
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

    private PublicationResult enterPublisher() {
        synchronized (lifecycleLock) {
            if (publicationGate == PublicationGate.OPEN) {
                activePublishers++;
                return PublicationResult.PUBLISHED;
            }
            if (workerFailurePending) {
                return PublicationResult.PIPELINE_FAILED;
            }
        }
        return failureResult();
    }

    private PublicationResult failureResult() {
        return supervisor.snapshot().failure() == null
                ? PublicationResult.NOT_RUNNING
                : PublicationResult.PIPELINE_FAILED;
    }

    private PublicationResult publicationState() {
        synchronized (lifecycleLock) {
            if (workerFailurePending) {
                return PublicationResult.PIPELINE_FAILED;
            }
            if (publicationGate == PublicationGate.OPEN) {
                return null;
            }
        }
        return failureResult();
    }

    private PublicationResult beforePublicationAttempt(
            long deadlineNanos,
            boolean allowExpiredAttempt)
            throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("等待管道 '" + name + "' 发布容量时被中断");
        }
        PublicationResult state = publicationState();
        if (state != null) {
            return state;
        }
        return !allowExpiredAttempt && remainingNanos(deadlineNanos) == 0L
                ? PublicationResult.TIMED_OUT
                : null;
    }

    private PublicationResult awaitPublicationRetry(long deadlineNanos)
            throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("等待管道 '" + name + "' 发布容量时被中断");
        }
        PublicationResult publicationState = publicationState();
        if (publicationState != null) {
            return publicationState;
        }
        long remaining = remainingNanos(deadlineNanos);
        if (remaining == 0L) {
            return PublicationResult.TIMED_OUT;
        }
        LockSupport.parkNanos(this, Math.min(PUBLICATION_RETRY_NANOS, remaining));
        if (Thread.interrupted()) {
            throw new InterruptedException("等待管道 '" + name + "' 发布容量时被中断");
        }
        return null;
    }

    private static long publicationDeadline(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout 不能为空");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout 不能为负数，实际值=" + timeout);
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long now = System.nanoTime();
        try {
            return Math.addExact(now, timeoutNanos);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static long remainingNanos(long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        return remaining > 0L ? remaining : 0L;
    }

    private void registerPublicationWaiter(Thread publisher) {
        synchronized (lifecycleLock) {
            publicationWaiters.add(publisher);
        }
    }

    private void unregisterPublicationWaiter(Thread publisher) {
        synchronized (lifecycleLock) {
            publicationWaiters.remove(publisher);
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
            closePublicationGateLocked();
        }
    }

    private void closePublicationGateAfterWorkerExit() {
        synchronized (lifecycleLock) {
            if (publicationGate == PublicationGate.OPEN) {
                workerFailurePending = true;
            }
            closePublicationGateLocked();
        }
    }

    private void closePublicationGateLocked() {
        publicationGate = PublicationGate.CLOSED;
        for (Thread waiter : publicationWaiters) {
            LockSupport.unpark(waiter);
        }
        lifecycleLock.notifyAll();
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
        public PublicationResult tryPublishEvent(EventTranslator<E> translator) {
            Objects.requireNonNull(translator, "translator 不能为空");
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            try {
                return ringBuffer.tryPublishEvent(translator)
                        ? PublicationResult.PUBLISHED
                        : PublicationResult.CAPACITY_EXHAUSTED;
            } finally {
                exitPublisher();
            }
        }

        @Override
        public PublicationResult publishEvent(EventTranslator<E> translator, Duration timeout)
                throws InterruptedException {
            Objects.requireNonNull(translator, "translator 不能为空");
            long deadlineNanos = publicationDeadline(timeout);
            boolean allowExpiredAttempt = timeout.isZero();
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            Thread publisher = Thread.currentThread();
            registerPublicationWaiter(publisher);
            try {
                while (true) {
                    PublicationResult state = beforePublicationAttempt(
                            deadlineNanos, allowExpiredAttempt);
                    allowExpiredAttempt = false;
                    if (state != null) {
                        return state;
                    }
                    if (ringBuffer.tryPublishEvent(translator)) {
                        return PublicationResult.PUBLISHED;
                    }
                    state = awaitPublicationRetry(deadlineNanos);
                    if (state != null) {
                        return state;
                    }
                }
            } finally {
                unregisterPublicationWaiter(publisher);
                exitPublisher();
            }
        }

        @Override
        public <A> PublicationResult tryPublishEvent(
                EventTranslatorOneArg<E, A> translator, A arg0) {
            Objects.requireNonNull(translator, "translator 不能为空");
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            try {
                return ringBuffer.tryPublishEvent(translator, arg0)
                        ? PublicationResult.PUBLISHED
                        : PublicationResult.CAPACITY_EXHAUSTED;
            } finally {
                exitPublisher();
            }
        }

        @Override
        public <A> PublicationResult publishEvent(
                EventTranslatorOneArg<E, A> translator, A arg0, Duration timeout)
                throws InterruptedException {
            Objects.requireNonNull(translator, "translator 不能为空");
            long deadlineNanos = publicationDeadline(timeout);
            boolean allowExpiredAttempt = timeout.isZero();
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            Thread publisher = Thread.currentThread();
            registerPublicationWaiter(publisher);
            try {
                while (true) {
                    PublicationResult state = beforePublicationAttempt(
                            deadlineNanos, allowExpiredAttempt);
                    allowExpiredAttempt = false;
                    if (state != null) {
                        return state;
                    }
                    if (ringBuffer.tryPublishEvent(translator, arg0)) {
                        return PublicationResult.PUBLISHED;
                    }
                    state = awaitPublicationRetry(deadlineNanos);
                    if (state != null) {
                        return state;
                    }
                }
            } finally {
                unregisterPublicationWaiter(publisher);
                exitPublisher();
            }
        }

        @Override
        public <A, B> PublicationResult tryPublishEvent(
                EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
            Objects.requireNonNull(translator, "translator 不能为空");
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            try {
                return ringBuffer.tryPublishEvent(translator, arg0, arg1)
                        ? PublicationResult.PUBLISHED
                        : PublicationResult.CAPACITY_EXHAUSTED;
            } finally {
                exitPublisher();
            }
        }

        @Override
        public <A, B> PublicationResult publishEvent(
                EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1, Duration timeout)
                throws InterruptedException {
            Objects.requireNonNull(translator, "translator 不能为空");
            long deadlineNanos = publicationDeadline(timeout);
            boolean allowExpiredAttempt = timeout.isZero();
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            Thread publisher = Thread.currentThread();
            registerPublicationWaiter(publisher);
            try {
                while (true) {
                    PublicationResult state = beforePublicationAttempt(
                            deadlineNanos, allowExpiredAttempt);
                    allowExpiredAttempt = false;
                    if (state != null) {
                        return state;
                    }
                    if (ringBuffer.tryPublishEvent(translator, arg0, arg1)) {
                        return PublicationResult.PUBLISHED;
                    }
                    state = awaitPublicationRetry(deadlineNanos);
                    if (state != null) {
                        return state;
                    }
                }
            } finally {
                unregisterPublicationWaiter(publisher);
                exitPublisher();
            }
        }

        @Override
        public <A, B, C> PublicationResult tryPublishEvent(
                EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2) {
            Objects.requireNonNull(translator, "translator 不能为空");
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            try {
                return ringBuffer.tryPublishEvent(translator, arg0, arg1, arg2)
                        ? PublicationResult.PUBLISHED
                        : PublicationResult.CAPACITY_EXHAUSTED;
            } finally {
                exitPublisher();
            }
        }

        @Override
        public <A, B, C> PublicationResult publishEvent(
                EventTranslatorThreeArg<E, A, B, C> translator,
                A arg0, B arg1, C arg2, Duration timeout) throws InterruptedException {
            Objects.requireNonNull(translator, "translator 不能为空");
            long deadlineNanos = publicationDeadline(timeout);
            boolean allowExpiredAttempt = timeout.isZero();
            PublicationResult admission = enterPublisher();
            if (admission != PublicationResult.PUBLISHED) {
                return admission;
            }
            Thread publisher = Thread.currentThread();
            registerPublicationWaiter(publisher);
            try {
                while (true) {
                    PublicationResult state = beforePublicationAttempt(
                            deadlineNanos, allowExpiredAttempt);
                    allowExpiredAttempt = false;
                    if (state != null) {
                        return state;
                    }
                    if (ringBuffer.tryPublishEvent(translator, arg0, arg1, arg2)) {
                        return PublicationResult.PUBLISHED;
                    }
                    state = awaitPublicationRetry(deadlineNanos);
                    if (state != null) {
                        return state;
                    }
                }
            } finally {
                unregisterPublicationWaiter(publisher);
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
