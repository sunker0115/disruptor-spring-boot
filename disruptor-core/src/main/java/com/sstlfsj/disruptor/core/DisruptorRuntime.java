package com.sstlfsj.disruptor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    private final Map<String, ManagedPipeline<?>> pipelinesByName;
    private final List<DisruptorPipeline<?>> pipelines;
    private final List<PipelineHandle<?>> handles;
    private final Duration shutdownTimeout;
    private final Object lifecycleLock = new Object();
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final CompletionStage<Void> startedView = started.minimalCompletionStage();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final CompletionStage<Void> terminationView = termination.minimalCompletionStage();

    private State state = State.NEW;
    private CompletableFuture<Void> shutdownOutcome;
    private CompletionStage<Void> shutdownOutcomeView;
    private ShutdownDeadline shutdownDeadline;
    private ShutdownMode shutdownMode;
    private boolean terminationAggregationStarted;

    private DisruptorRuntime(
            Collection<PipelineSpec<?>> specs,
            Function<String, PipelineSettings> settingsResolver,
            Duration shutdownTimeout) {
        validateShutdownTimeout(shutdownTimeout);
        Map<String, ManagedPipeline<?>> built = new LinkedHashMap<>();
        for (PipelineSpec<?> spec : specs) {
            Objects.requireNonNull(spec, "PipelineSpec 不能为空");
            if (built.containsKey(spec.name())) {
                throw new IllegalArgumentException("管道名重复：" + spec.name());
            }
            PipelineSettings settings = Objects.requireNonNull(settingsResolver.apply(spec.name()),
                    "settingsResolver 不能为管道 " + spec.name() + " 返回 null");
            built.put(spec.name(), buildPipeline(spec, settings, shutdownTimeout));
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
        ManagedPipeline<?> pipeline = pipelinesByName.get(name);
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
        List<ManagedPipeline<?>> matches = pipelinesByName.values().stream()
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
            for (ManagedPipeline<?> pipeline : pipelinesByName.values()) {
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
        ShutdownDeadline deadline;
        synchronized (lifecycleLock) {
            state = State.STOPPING;
            if (shutdownDeadline == null) {
                shutdownDeadline = ShutdownDeadline.after(shutdownTimeout);
            }
            deadline = shutdownDeadline;
            shutdownMode = ShutdownMode.IMMEDIATE;
            startTerminationAggregationLocked();
        }
        requestEveryPipeline(ShutdownMode.IMMEDIATE, deadline);
        boolean allTerminated = awaitChildren(deadline);
        IllegalStateException result = new IllegalStateException(
                "启动 Disruptor 管道失败，已请求回滚全部管道", startupFailure);
        if (!allTerminated) {
            result.addSuppressed(aggregateFailure("启动失败回滚超过共享关闭预算"));
        }
        started.completeExceptionally(result);
    }

    private CompletionStage<Void> requestStop(ShutdownMode requestedMode) {
        ShutdownDeadline deadline;
        boolean startCoordinator = false;
        boolean upgrade;
        synchronized (lifecycleLock) {
            if (state == State.TERMINATED) {
                return shutdownOutcomeView == null
                        ? CompletableFuture.completedStage(null)
                        : shutdownOutcomeView;
            }
            if (shutdownOutcome == null) {
                shutdownOutcome = new CompletableFuture<>();
                shutdownOutcomeView = shutdownOutcome.minimalCompletionStage();
                shutdownDeadline = ShutdownDeadline.after(shutdownTimeout);
                shutdownMode = requestedMode;
                state = State.STOPPING;
                startTerminationAggregationLocked();
                startCoordinator = true;
            }
            upgrade = requestedMode == ShutdownMode.IMMEDIATE
                    && shutdownMode != ShutdownMode.IMMEDIATE;
            if (upgrade) {
                shutdownMode = ShutdownMode.IMMEDIATE;
            }
            deadline = shutdownDeadline;
        }
        if (upgrade) {
            requestEveryPipeline(ShutdownMode.IMMEDIATE, deadline);
        }
        if (startCoordinator) {
            ShutdownMode initialMode = requestedMode;
            Thread.ofVirtual().name("disruptor-runtime-shutdown")
                    .start(() -> stopPipelines(initialMode, deadline));
        }
        return shutdownOutcomeView;
    }

    private void stopPipelines(ShutdownMode initialMode, ShutdownDeadline deadline) {
        requestEveryPipeline(initialMode, deadline);
        if (awaitChildren(deadline)) {
            completeShutdownFromChildren(initialMode);
            return;
        }
        requestEveryPipeline(ShutdownMode.IMMEDIATE, deadline);
        shutdownOutcome.completeExceptionally(
                aggregateFailure("DisruptorRuntime 超过共享关闭预算 " + shutdownTimeout));
    }

    private void completeShutdownFromChildren(ShutdownMode initialMode) {
        List<PipelineSnapshot> failed = pipelines.stream()
                .map(DisruptorPipeline::snapshot)
                .filter(snapshot -> snapshot.failure() != null)
                .toList();
        if (failed.isEmpty()) {
            shutdownOutcome.complete(null);
            return;
        }
        String action = initialMode == ShutdownMode.GRACEFUL ? "优雅关闭" : "立即停止";
        shutdownOutcome.completeExceptionally(aggregateFailure("DisruptorRuntime " + action + "失败"));
    }

    private void requestEveryPipeline(ShutdownMode mode, ShutdownDeadline deadline) {
        for (DisruptorPipeline<?> pipeline : pipelines) {
            try {
                pipeline.requestShutdown(mode, deadline);
            } catch (Throwable failure) {
                log.warn("请求停止 Disruptor 管道 [{}] 失败", pipeline.handle().name(), failure);
            }
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

    private void startTerminationAggregationLocked() {
        if (terminationAggregationStarted) {
            return;
        }
        terminationAggregationStarted = true;
        CompletableFuture<?>[] childTerminations = pipelines.stream()
                .map(DisruptorPipeline::termination)
                .map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(childTerminations).whenComplete((ignored, failure) -> {
            synchronized (lifecycleLock) {
                state = State.TERMINATED;
            }
            if (failure == null) {
                termination.complete(null);
            } else {
                termination.completeExceptionally(unwrap(failure));
            }
        });
    }

    private DisruptorShutdownException aggregateFailure(String message) {
        DisruptorShutdownException aggregate = new DisruptorShutdownException(message);
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (DisruptorPipeline<?> pipeline : pipelines) {
            Throwable failure = pipeline.snapshot().failure();
            if (failure != null && seen.add(failure)) {
                aggregate.addSuppressed(failure);
            }
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

    @SuppressWarnings("unchecked")
    private static <E> DisruptorPipeline<E> castPipeline(ManagedPipeline<?> pipeline) {
        return (DisruptorPipeline<E>) pipeline;
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
