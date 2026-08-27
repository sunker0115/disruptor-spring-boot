package com.sstlfsj.disruptor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * 多条命名管道的纯 Java 运行时，负责构建、注册和一次性生命周期。
 */
public final class DisruptorRuntime {

    private static final Logger log = LoggerFactory.getLogger(DisruptorRuntime.class);

    private final Map<String, ManagedPipeline<?>> handlesByName;
    private final List<PipelineHandle<?>> handles;
    private final Duration shutdownTimeout;
    private volatile State state = State.NEW;

    private DisruptorRuntime(Collection<PipelineSpec<?>> specs,
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
            built.put(spec.name(), buildHandle(spec, settings));
        }
        this.handlesByName = Collections.unmodifiableMap(built);
        this.handles = List.copyOf(built.values());
        this.shutdownTimeout = shutdownTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<PipelineHandle<?>> handles() {
        return handles;
    }

    public <E> PipelineHandle<E> require(String name, Class<E> eventType) {
        PipelineSettings.requireName(name);
        Objects.requireNonNull(eventType, "eventType 不能为空");
        ManagedPipeline<?> handle = handlesByName.get(name);
        if (handle == null) {
            throw new IllegalArgumentException("不存在名为 '" + name + "' 的管道");
        }
        if (!handle.eventType().equals(eventType)) {
            throw new IllegalArgumentException("管道 '" + name + "' 的事件类型是 "
                    + handle.eventType().getName() + "，不是 " + eventType.getName());
        }
        return cast(handle);
    }

    public <E> PipelineHandle<E> unique(Class<E> eventType) {
        Objects.requireNonNull(eventType, "eventType 不能为空");
        List<ManagedPipeline<?>> matches = handlesByName.values().stream()
                .filter(handle -> handle.eventType().equals(eventType))
                .toList();
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("不存在事件类型为 " + eventType.getName() + " 的管道");
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("事件类型 " + eventType.getName()
                    + " 对应多条管道，请按名称获取："
                    + matches.stream().map(PipelineHandle::name).toList());
        }
        return cast(matches.get(0));
    }

    public synchronized void start() {
        if (state == State.RUNNING) {
            return;
        }
        if (state == State.STOPPED) {
            throw new IllegalStateException("DisruptorRuntime 已停止，LMAX Disruptor 不支持重新启动");
        }

        state = State.STARTING;
        List<ManagedPipeline<?>> attempted = new ArrayList<>();
        try {
            for (ManagedPipeline<?> handle : handlesByName.values()) {
                attempted.add(handle);
                handle.start();
                log.info("已启动 Disruptor 管道 [{}]，事件类型={}，bufferSize={}",
                        handle.name(), handle.eventType().getName(),
                        handle.unsafeRingBuffer().getBufferSize());
            }
            state = State.RUNNING;
        } catch (RuntimeException | Error failure) {
            Collections.reverse(attempted);
            long deadlineNanos = deadlineNanos();
            for (ManagedPipeline<?> handle : attempted) {
                ManagedPipeline.StopResult result = handle.haltNow(deadlineNanos);
                if (!result.isSuccessful()) {
                    Throwable cleanupFailure = result.cause() == null
                            ? new IllegalStateException("回滚管道 '" + handle.name()
                            + "' 未完整完成：" + result.message())
                            : result.cause();
                    failure.addSuppressed(cleanupFailure);
                }
            }
            state = State.STOPPED;
            throw new IllegalStateException("启动 Disruptor 管道失败，已回滚已启动管道", failure);
        }
    }

    public synchronized void shutdown() {
        if (state == State.STOPPED) {
            return;
        }
        if (state == State.NEW) {
            state = State.STOPPED;
            return;
        }

        state = State.QUIESCING;
        long deadlineNanos = deadlineNanos();
        List<ManagedPipeline<?>> reverse = new ArrayList<>(handlesByName.values());
        Collections.reverse(reverse);
        List<String> failures = new ArrayList<>();
        DisruptorShutdownException aggregate = new DisruptorShutdownException(
                "DisruptorRuntime 未能在 " + shutdownTimeout + " 内完整关闭");
        try {
            for (ManagedPipeline<?> handle : handlesByName.values()) {
                handle.quiesce();
            }
            state = State.STOPPING;
            for (ManagedPipeline<?> handle : reverse) {
                ManagedPipeline.StopResult result = handle.shutdown(deadlineNanos);
                if (result.isGraceful()) {
                    log.info("已停止 Disruptor 管道 [{}]", handle.name());
                    continue;
                }
                failures.add(handle.name() + ": " + result.message());
                if (result.cause() != null) {
                    aggregate.addSuppressed(result.cause());
                }
                log.warn("停止 Disruptor 管道 [{}] 未完整完成：{}",
                        handle.name(), result.message(), result.cause());
            }
        } finally {
            state = State.STOPPED;
        }
        if (!failures.isEmpty()) {
            DisruptorShutdownException failure = new DisruptorShutdownException(
                    aggregate.getMessage() + "：" + failures);
            for (Throwable suppressed : aggregate.getSuppressed()) {
                failure.addSuppressed(suppressed);
            }
            throw failure;
        }
    }

    public synchronized void halt() {
        if (state == State.STOPPED) {
            return;
        }
        state = State.STOPPING;
        long deadlineNanos = deadlineNanos();
        List<ManagedPipeline<?>> reverse = new ArrayList<>(handlesByName.values());
        Collections.reverse(reverse);
        List<String> failures = new ArrayList<>();
        DisruptorShutdownException aggregate = new DisruptorShutdownException(
                "DisruptorRuntime 未能在 " + shutdownTimeout + " 内强制停止");
        try {
            for (ManagedPipeline<?> handle : handlesByName.values()) {
                handle.quiesce();
            }
            for (ManagedPipeline<?> handle : reverse) {
                ManagedPipeline.StopResult result = handle.haltNow(deadlineNanos);
                if (!result.isSuccessful()) {
                    failures.add(handle.name() + ": " + result.message());
                    if (result.cause() != null) {
                        aggregate.addSuppressed(result.cause());
                    }
                    log.warn("强制停止 Disruptor 管道 [{}] 未完整完成：{}",
                            handle.name(), result.message(), result.cause());
                }
            }
        } finally {
            state = State.STOPPED;
        }
        if (!failures.isEmpty()) {
            DisruptorShutdownException failure = new DisruptorShutdownException(
                    aggregate.getMessage() + "：" + failures);
            for (Throwable suppressed : aggregate.getSuppressed()) {
                failure.addSuppressed(suppressed);
            }
            throw failure;
        }
    }

    /**
     * Runtime 是否处于已启动且未停止的托管生命周期状态；不代表每个消费者线程都健康。
     */
    public boolean isRunning() {
        return state == State.RUNNING;
    }

    private long deadlineNanos() {
        return System.nanoTime() + shutdownTimeout.toNanos();
    }

    private static void validateShutdownTimeout(Duration shutdownTimeout) {
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout 不能为空");
        if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout 必须大于 0，实际值=" + shutdownTimeout);
        }
        shutdownTimeout.toNanos();
    }

    private static <E> ManagedPipeline<E> buildHandle(PipelineSpec<E> spec,
                                                       PipelineSettings settings) {
        return ManagedPipeline.build(spec, spec.resolve(settings));
    }

    @SuppressWarnings("unchecked")
    private static <E> PipelineHandle<E> cast(ManagedPipeline<?> handle) {
        return (PipelineHandle<E>) handle;
    }

    private enum State {
        NEW,
        STARTING,
        RUNNING,
        QUIESCING,
        STOPPING,
        STOPPED
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
