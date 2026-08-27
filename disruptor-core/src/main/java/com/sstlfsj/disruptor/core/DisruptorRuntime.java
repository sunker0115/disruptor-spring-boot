package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final Map<String, DefaultPipelineHandle<?>> handlesByName;
    private final List<PipelineHandle<?>> handles;
    private State state = State.NEW;

    private DisruptorRuntime(Collection<PipelineSpec<?>> specs,
                             Function<String, PipelineSettings> settingsResolver) {
        Map<String, DefaultPipelineHandle<?>> built = new LinkedHashMap<>();
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
        DefaultPipelineHandle<?> handle = handlesByName.get(name);
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
        List<DefaultPipelineHandle<?>> matches = handlesByName.values().stream()
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

        List<DefaultPipelineHandle<?>> attempted = new ArrayList<>();
        try {
            for (DefaultPipelineHandle<?> handle : handlesByName.values()) {
                attempted.add(handle);
                handle.start();
                log.info("已启动 Disruptor 管道 [{}]，事件类型={}，bufferSize={}",
                        handle.name(), handle.eventType().getName(), handle.ringBuffer().getBufferSize());
            }
            state = State.RUNNING;
        } catch (RuntimeException | Error failure) {
            Collections.reverse(attempted);
            for (DefaultPipelineHandle<?> handle : attempted) {
                try {
                    handle.halt();
                } catch (RuntimeException | Error cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                    log.warn("回滚 Disruptor 管道 [{}] 失败", handle.name(), cleanupFailure);
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

        List<DefaultPipelineHandle<?>> reverse = new ArrayList<>(handlesByName.values());
        Collections.reverse(reverse);
        try {
            for (DefaultPipelineHandle<?> handle : reverse) {
                try {
                    handle.shutdown();
                    log.info("已停止 Disruptor 管道 [{}]", handle.name());
                } catch (TimeoutException timeout) {
                    log.warn("Disruptor 管道 [{}] 在 {} 内未排空，已强制 halt",
                            handle.name(), handle.shutdownTimeout());
                    haltAfterShutdownFailure(handle, timeout);
                } catch (RuntimeException failure) {
                    log.warn("停止 Disruptor 管道 [{}] 失败，已强制 halt", handle.name(), failure);
                    haltAfterShutdownFailure(handle, failure);
                }
            }
        } finally {
            state = State.STOPPED;
        }
    }

    public synchronized void halt() {
        if (state == State.STOPPED) {
            return;
        }
        List<DefaultPipelineHandle<?>> reverse = new ArrayList<>(handlesByName.values());
        Collections.reverse(reverse);
        try {
            for (DefaultPipelineHandle<?> handle : reverse) {
                try {
                    handle.halt();
                } catch (RuntimeException failure) {
                    log.warn("强制停止 Disruptor 管道 [{}] 失败", handle.name(), failure);
                }
            }
        } finally {
            state = State.STOPPED;
        }
    }

    /**
     * Runtime 是否处于已启动且未停止的托管生命周期状态；不代表每个消费者线程都健康。
     */
    public synchronized boolean isRunning() {
        return state == State.RUNNING;
    }

    private static void haltAfterShutdownFailure(DefaultPipelineHandle<?> handle, Throwable failure) {
        try {
            handle.halt();
        } catch (RuntimeException haltFailure) {
            failure.addSuppressed(haltFailure);
            log.warn("强制停止 Disruptor 管道 [{}] 仍然失败", handle.name(), haltFailure);
        }
    }

    private static <E> DefaultPipelineHandle<E> buildHandle(PipelineSpec<E> spec,
                                                             PipelineSettings settings) {
        ResolvedPipelineSettings<E> resolved = spec.resolve(settings);
        Disruptor<E> disruptor = new Disruptor<>(
                spec.eventFactory(),
                resolved.bufferSize(),
                resolved.threadFactory(),
                resolved.producerType(),
                resolved.waitStrategy());
        // 统一在 topology 装配之前设置默认异常处理器(经 ExceptionHandlerWrapper,对存量+新建 processor 均生效)。
        disruptor.setDefaultExceptionHandler(resolved.exceptionHandler());
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
        return new DefaultPipelineHandle<>(spec.name(), spec.eventType(), disruptor,
                resolved.shutdownTimeout());
    }

    @SuppressWarnings("unchecked")
    private static <E> PipelineHandle<E> cast(DefaultPipelineHandle<?> handle) {
        return (PipelineHandle<E>) handle;
    }

    private enum State {
        NEW,
        RUNNING,
        STOPPED
    }

    public static final class Builder {

        private final List<PipelineSpec<?>> specs = new ArrayList<>();
        private Function<String, PipelineSettings> settingsResolver = name -> PipelineSettings.defaults();

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

        public DisruptorRuntime build() {
            return new DisruptorRuntime(List.copyOf(specs), settingsResolver);
        }
    }

    private static final class DefaultPipelineHandle<E> implements PipelineHandle<E> {

        private final String name;
        private final Class<E> eventType;
        private final Disruptor<E> disruptor;
        private final RingBuffer<E> ringBuffer;
        private final java.time.Duration shutdownTimeout;

        private DefaultPipelineHandle(String name, Class<E> eventType, Disruptor<E> disruptor,
                                      java.time.Duration shutdownTimeout) {
            this.name = name;
            this.eventType = eventType;
            this.disruptor = disruptor;
            this.ringBuffer = disruptor.getRingBuffer();
            this.shutdownTimeout = shutdownTimeout;
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
        public Disruptor<E> disruptor() {
            return disruptor;
        }

        @Override
        public RingBuffer<E> ringBuffer() {
            return ringBuffer;
        }

        @Override
        public boolean hasStarted() {
            return disruptor.hasStarted();
        }

        private java.time.Duration shutdownTimeout() {
            return shutdownTimeout;
        }

        private void start() {
            disruptor.start();
        }

        private void shutdown() throws TimeoutException {
            disruptor.shutdown(shutdownTimeout.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
        }

        private void halt() {
            disruptor.halt();
        }
    }
}
