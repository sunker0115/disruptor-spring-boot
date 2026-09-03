package com.sstlfsj.disruptor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/** 多条命名 {@link DisruptorPipeline} 的启动、共享截止时间关闭和真实终止聚合器。 */
public final class DisruptorRuntime {

    private static final Logger log = LoggerFactory.getLogger(DisruptorRuntime.class);
    private static final long WAIT_NANOS = 100_000L;

    private final Map<String, DisruptorPipeline<?>> pipelinesByName;
    private final List<DisruptorPipeline<?>> pipelines;
    private final List<PipelineHandle<?>> handles;
    private final Duration shutdownTimeout;
    private final Object lifecycleLock = new Object();
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final CompletionStage<Void> startedView = started.minimalCompletionStage();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final CompletionStage<Void> terminationView = termination.minimalCompletionStage();

    private State state = State.NEW;
    private RuntimeShutdownSession shutdownSession;

    private DisruptorRuntime(
            Collection<PipelineSpec<?>> specs,
            Function<String, PipelineSettings> settingsResolver,
            Duration shutdownTimeout) {
        this(buildPipelines(specs, settingsResolver, shutdownTimeout), shutdownTimeout);
    }

    DisruptorRuntime(
            Collection<? extends DisruptorPipeline<?>> pipelines,
            Duration shutdownTimeout) {
        validateShutdownTimeout(shutdownTimeout);
        Map<String, DisruptorPipeline<?>> built = new LinkedHashMap<>();
        for (DisruptorPipeline<?> pipeline : Objects.requireNonNull(
                pipelines, "pipelines 不能为空")) {
            Objects.requireNonNull(pipeline, "DisruptorPipeline 不能为空");
            PipelineHandle<?> handle = Objects.requireNonNull(
                    pipeline.handle(), "pipeline.handle() 不能返回 null");
            if (built.putIfAbsent(handle.name(), pipeline) != null) {
                throw new IllegalArgumentException("管道名重复：" + handle.name());
            }
        }
        this.pipelinesByName = Collections.unmodifiableMap(built);
        this.pipelines = List.copyOf(built.values());
        List<PipelineHandle<?>> builtHandles = new ArrayList<>();
        built.values().forEach(pipeline -> builtHandles.add(pipeline.handle()));
        this.handles = List.copyOf(builtHandles);
        this.shutdownTimeout = shutdownTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<DisruptorPipeline<?>> pipelines() {
        return pipelines;
    }

    public List<PipelineHandle<?>> handles() {
        return handles;
    }

    public <E> PipelineHandle<E> require(String name, Class<E> eventType) {
        return requirePipeline(name, eventType).handle();
    }

    public <E> PipelineHandle<E> unique(Class<E> eventType) {
        return uniquePipeline(eventType).handle();
    }

    public <E> DisruptorPipeline<E> requirePipeline(String name, Class<E> eventType) {
        PipelineSettings.requireName(name);
        Objects.requireNonNull(eventType, "eventType 不能为空");
        DisruptorPipeline<?> pipeline = pipelinesByName.get(name);
        if (pipeline == null) {
            throw new IllegalArgumentException("不存在名为 '" + name + "' 的管道");
        }
        if (!pipeline.handle().eventType().equals(eventType)) {
            throw new IllegalArgumentException("管道 '" + name + "' 的事件类型是 "
                    + pipeline.handle().eventType().getName() + "，不是 " + eventType.getName());
        }
        return castPipeline(pipeline);
    }

    public <E> DisruptorPipeline<E> uniquePipeline(Class<E> eventType) {
        Objects.requireNonNull(eventType, "eventType 不能为空");
        List<DisruptorPipeline<?>> matches = pipelinesByName.values().stream()
                .filter(pipeline -> pipeline.handle().eventType().equals(eventType))
                .toList();
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("不存在事件类型为 " + eventType.getName() + " 的管道");
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("事件类型 " + eventType.getName()
                    + " 对应多条管道，请按名称获取："
                    + matches.stream().map(pipeline -> pipeline.handle().name()).toList());
        }
        return castPipeline(matches.get(0));
    }

    /** 同步启动；返回前所有 child worker 都已经入场且管道已进入 RUNNING。 */
    public void start() {
        awaitOperation(startAsync());
    }

    /** 异步启动；同一次启动只创建一个协调动作。 */
    public CompletionStage<Void> startAsync() {
        synchronized (lifecycleLock) {
            if (state == State.RUNNING || state == State.STARTING) {
                return startedView;
            }
            if (state == State.STOPPING || state == State.TERMINATED) {
                throw new IllegalStateException("DisruptorRuntime 已停止，LMAX Disruptor 不支持重新启动");
            }
            state = State.STARTING;
            Thread.ofVirtual().name("disruptor-runtime-start").start(this::startPipelines);
            return startedView;
        }
    }

    /** 同步优雅关闭；预算到期时抛出聚合异常，后台仍继续等待真实终止。 */
    public void shutdown() {
        awaitOperation(requestStop(ShutdownMode.GRACEFUL));
    }

    public CompletionStage<Void> shutdownAsync() {
        return requestStop(ShutdownMode.GRACEFUL);
    }

    /** 同步立即停止；仍受一次共享截止时间约束。 */
    public void halt() {
        awaitOperation(requestStop(ShutdownMode.IMMEDIATE));
    }

    public CompletionStage<Void> haltAsync() {
        return requestStop(ShutdownMode.IMMEDIATE);
    }

    /** 只在全部 child 实际终止后完成，与关闭调用是否已经超时返回无关。 */
    public CompletionStage<Void> termination() {
        return terminationView;
    }

    public boolean isRunning() {
        synchronized (lifecycleLock) {
            return state == State.RUNNING && pipelines.stream()
                    .allMatch(pipeline -> pipeline.snapshot().lifecycle() == PipelineLifecycle.RUNNING);
        }
    }

    private void startPipelines() {
        try {
            for (DisruptorPipeline<?> pipeline : pipelinesByName.values()) {
                pipeline.start().toCompletableFuture().join();
                PipelineHandle<?> handle = pipeline.handle();
                log.info("已启动 Disruptor 管道 [{}]，事件类型={}，bufferSize={}",
                        handle.name(), handle.eventType().getName(),
                        handle.unsafeRingBuffer().getBufferSize());
            }
            synchronized (lifecycleLock) {
                if (state != State.STARTING) {
                    throw new IllegalStateException("Runtime 在启动完成前已收到关闭请求");
                }
                state = State.RUNNING;
            }
            started.complete(null);
        } catch (Throwable failure) {
            rollbackStartup(unwrap(failure));
        }
    }

    private void rollbackStartup(Throwable startupFailure) {
        ShutdownRequest request;
        synchronized (lifecycleLock) {
            request = beginShutdownLocked(ShutdownMode.IMMEDIATE);
            request.session().startupFailures.add(startupFailure);
            request.session().startupPending = false;
        }
        if (request.broadcast()) {
            requestEveryPipeline(request.session(), ShutdownMode.IMMEDIATE);
        }
        startShutdownCoordinator(request);
        boolean allTerminated = awaitChildren(request.session().deadline);
        IllegalStateException result = new IllegalStateException(
                "启动 Disruptor 管道失败，已请求回滚全部管道", startupFailure);
        if (!allTerminated) {
            result.addSuppressed(aggregateFailure(
                    request.session(), "启动失败回滚超过共享关闭预算"));
        }
        started.completeExceptionally(result);
    }

    private CompletionStage<Void> requestStop(ShutdownMode requestedMode) {
        ShutdownRequest request;
        synchronized (lifecycleLock) {
            if (state == State.TERMINATED) {
                return shutdownSession.outcomeView;
            }
            request = beginShutdownLocked(requestedMode);
        }
        requestEveryPipeline(request.session(), request.modeToBroadcast());
        startShutdownCoordinator(request);
        return request.session().outcomeView;
    }

    private ShutdownRequest beginShutdownLocked(ShutdownMode requestedMode) {
        if (shutdownSession == null) {
            shutdownSession = new RuntimeShutdownSession(
                    ShutdownDeadline.after(shutdownTimeout),
                    requestedMode,
                    state == State.STARTING);
        }
        shutdownSession.upgrade(requestedMode);
        boolean startCoordinator = !shutdownSession.coordinationStarted;
        shutdownSession.coordinationStarted = true;
        boolean broadcast = state != State.TERMINATED;
        if (broadcast) {
            state = State.STOPPING;
        }
        return new ShutdownRequest(
                shutdownSession,
                shutdownSession.highestMode,
                startCoordinator,
                broadcast);
    }

    private void startShutdownCoordinator(ShutdownRequest request) {
        if (request.startCoordinator()) {
            Thread.ofVirtual().name("disruptor-runtime-shutdown")
                    .start(() -> coordinateShutdown(request.session()));
        }
    }

    private void coordinateShutdown(RuntimeShutdownSession session) {
        if (!awaitShutdownOutcomeReady(session)) {
            synchronized (lifecycleLock) {
                session.upgrade(ShutdownMode.IMMEDIATE);
            }
            requestUnterminatedPipelines(session, ShutdownMode.IMMEDIATE);
            session.outcome.completeExceptionally(aggregateFailure(
                    session, "DisruptorRuntime 超过共享关闭预算 " + shutdownTimeout));
        }
        awaitEveryChildTermination();
        List<Throwable> terminationFailures = collectChildTerminationFailures();
        synchronized (lifecycleLock) {
            session.childTerminationFailures.addAll(terminationFailures);
            state = State.TERMINATED;
        }
        if (!session.outcome.isDone()) {
            completeShutdownFromChildren(session);
        }
        completeRuntimeTermination(terminationFailures);
    }

    private void requestEveryPipeline(RuntimeShutdownSession session, ShutdownMode mode) {
        for (DisruptorPipeline<?> pipeline : pipelines) {
            requestPipeline(session, pipeline, mode);
        }
    }

    private void requestUnterminatedPipelines(
            RuntimeShutdownSession session,
            ShutdownMode mode) {
        for (DisruptorPipeline<?> pipeline : pipelines) {
            if (!pipeline.termination().toCompletableFuture().isDone()) {
                requestPipeline(session, pipeline, mode);
            }
        }
    }

    private void requestPipeline(
            RuntimeShutdownSession session,
            DisruptorPipeline<?> pipeline,
            ShutdownMode mode) {
        try {
            pipeline.requestShutdown(mode, session.deadline);
        } catch (Throwable failure) {
            synchronized (lifecycleLock) {
                session.requestFailures.add(failure);
            }
            log.warn("请求停止 Disruptor 管道 [{}] 失败", pipeline.handle().name(), failure);
        }
    }

    private boolean awaitShutdownOutcomeReady(RuntimeShutdownSession session) {
        while (true) {
            boolean startupPending;
            synchronized (lifecycleLock) {
                startupPending = session.startupPending;
            }
            if (!startupPending && allChildrenTerminated()) {
                return true;
            }
            long remaining = session.deadline.remainingNanos();
            if (remaining == 0L) {
                return false;
            }
            LockSupport.parkNanos(Math.min(WAIT_NANOS, remaining));
        }
    }

    private boolean awaitChildren(ShutdownDeadline deadline) {
        while (!allChildrenTerminated()) {
            long remaining = deadline.remainingNanos();
            if (remaining == 0L) {
                return false;
            }
            LockSupport.parkNanos(Math.min(WAIT_NANOS, remaining));
        }
        return true;
    }

    private boolean allChildrenTerminated() {
        return pipelines.stream().allMatch(pipeline ->
                pipeline.termination().toCompletableFuture().isDone());
    }

    private void awaitEveryChildTermination() {
        while (!allChildrenTerminated()) {
            LockSupport.parkNanos(WAIT_NANOS);
        }
    }

    private List<Throwable> collectChildTerminationFailures() {
        List<Throwable> failures = new ArrayList<>();
        for (DisruptorPipeline<?> pipeline : pipelines) {
            try {
                pipeline.termination().toCompletableFuture().join();
            } catch (Throwable failure) {
                failures.add(unwrap(failure));
            }
        }
        return failures;
    }

    private void completeShutdownFromChildren(RuntimeShutdownSession session) {
        List<Throwable> failures = shutdownFailures(session);
        if (failures.isEmpty()) {
            session.outcome.complete(null);
            return;
        }
        ShutdownMode mode;
        synchronized (lifecycleLock) {
            mode = session.highestMode;
        }
        String action = mode == ShutdownMode.GRACEFUL ? "优雅关闭" : "立即停止";
        session.outcome.completeExceptionally(aggregateFailure(
                "DisruptorRuntime " + action + "失败", failures));
    }

    private void completeRuntimeTermination(List<Throwable> terminationFailures) {
        if (terminationFailures.isEmpty()) {
            termination.complete(null);
        } else {
            termination.completeExceptionally(aggregateFailure(
                    "DisruptorRuntime child 真实终止失败", terminationFailures));
        }
    }

    private DisruptorShutdownException aggregateFailure(
            RuntimeShutdownSession session,
            String message) {
        return aggregateFailure(message, shutdownFailures(session));
    }

    private List<Throwable> shutdownFailures(RuntimeShutdownSession session) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        synchronized (lifecycleLock) {
            seen.addAll(session.startupFailures);
            seen.addAll(session.requestFailures);
            seen.addAll(session.childTerminationFailures);
        }
        for (DisruptorPipeline<?> pipeline : pipelines) {
            Throwable failure = pipeline.snapshot().failure();
            if (failure != null) {
                seen.add(failure);
            }
        }
        return List.copyOf(seen);
    }

    private static DisruptorShutdownException aggregateFailure(
            String message,
            Collection<? extends Throwable> failures) {
        DisruptorShutdownException aggregate = new DisruptorShutdownException(message);
        for (Throwable failure : failures) {
            aggregate.addSuppressed(failure);
        }
        return aggregate;
    }

    private static void awaitOperation(CompletionStage<Void> operation) {
        try {
            operation.toCompletableFuture().join();
        } catch (CompletionException failure) {
            Throwable cause = unwrap(failure);
            if (cause instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(cause);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void validateShutdownTimeout(Duration shutdownTimeout) {
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout 不能为空");
        if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout 必须大于 0，实际值=" + shutdownTimeout);
        }
        shutdownTimeout.toNanos();
    }

    private static <E> ManagedPipeline<E> buildPipeline(
            PipelineSpec<E> spec,
            PipelineSettings settings,
            Duration shutdownTimeout) {
        return ManagedPipeline.build(spec, spec.resolve(settings), shutdownTimeout);
    }

    private static List<DisruptorPipeline<?>> buildPipelines(
            Collection<PipelineSpec<?>> specs,
            Function<String, PipelineSettings> settingsResolver,
            Duration shutdownTimeout) {
        List<DisruptorPipeline<?>> pipelines = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (PipelineSpec<?> spec : Objects.requireNonNull(specs, "specs 不能为空")) {
            Objects.requireNonNull(spec, "PipelineSpec 不能为空");
            if (!names.add(spec.name())) {
                throw new IllegalArgumentException("管道名重复：" + spec.name());
            }
            PipelineSettings settings = Objects.requireNonNull(settingsResolver.apply(spec.name()),
                    "settingsResolver 不能为管道 " + spec.name() + " 返回 null");
            pipelines.add(buildPipeline(spec, settings, shutdownTimeout));
        }
        return pipelines;
    }

    @SuppressWarnings("unchecked")
    private static <E> DisruptorPipeline<E> castPipeline(DisruptorPipeline<?> pipeline) {
        return (DisruptorPipeline<E>) pipeline;
    }

    private record ShutdownRequest(
            RuntimeShutdownSession session,
            ShutdownMode modeToBroadcast,
            boolean startCoordinator,
            boolean broadcast) {
    }

    private static final class RuntimeShutdownSession {

        private final ShutdownDeadline deadline;
        private final CompletableFuture<Void> outcome = new CompletableFuture<>();
        private final CompletionStage<Void> outcomeView = outcome.minimalCompletionStage();
        private final Set<Throwable> startupFailures =
                Collections.newSetFromMap(new IdentityHashMap<>());
        private final Set<Throwable> requestFailures =
                Collections.newSetFromMap(new IdentityHashMap<>());
        private final Set<Throwable> childTerminationFailures =
                Collections.newSetFromMap(new IdentityHashMap<>());

        private ShutdownMode highestMode;
        private boolean coordinationStarted;
        private boolean startupPending;

        private RuntimeShutdownSession(
                ShutdownDeadline deadline,
                ShutdownMode initialMode,
                boolean startupPending) {
            this.deadline = deadline;
            this.highestMode = initialMode;
            this.startupPending = startupPending;
        }

        private void upgrade(ShutdownMode requestedMode) {
            if (requestedMode == ShutdownMode.IMMEDIATE) {
                highestMode = ShutdownMode.IMMEDIATE;
            }
        }
    }

    private enum State {
        NEW,
        STARTING,
        RUNNING,
        STOPPING,
        TERMINATED
    }

    public static final class Builder {

        private final List<PipelineSpec<?>> specs = new ArrayList<>();
        private Function<String, PipelineSettings> settingsResolver = name -> PipelineSettings.defaults();
        private Duration shutdownTimeout = Duration.ofSeconds(10);

        private Builder() {
        }

        public Builder add(PipelineSpec<?> spec) {
            specs.add(Objects.requireNonNull(spec, "PipelineSpec 不能为空"));
            return this;
        }

        public Builder addAll(Collection<? extends PipelineSpec<?>> specs) {
            this.specs.addAll(Objects.requireNonNull(specs, "specs 不能为空"));
            return this;
        }

        public Builder settings(PipelineSettings settings) {
            Objects.requireNonNull(settings, "settings 不能为空");
            this.settingsResolver = name -> settings;
            return this;
        }

        public Builder settingsResolver(Function<String, PipelineSettings> settingsResolver) {
            this.settingsResolver = Objects.requireNonNull(settingsResolver, "settingsResolver 不能为空");
            return this;
        }

        public Builder shutdownTimeout(Duration shutdownTimeout) {
            validateShutdownTimeout(shutdownTimeout);
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public DisruptorRuntime build() {
            return new DisruptorRuntime(List.copyOf(specs), settingsResolver, shutdownTimeout);
        }
    }
}
