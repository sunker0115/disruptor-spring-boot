package com.sstlfsj.disruptor.concurrent;

import java.time.Duration;
import java.util.Objects;

/** 构建拥有固定 child 集合的 EventLoopGroup。 */
public final class EventLoopGroupBuilder {

    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private final String name;
    private final int childCount;
    private final EventLoopFactory eventLoopFactory;
    private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

    private EventLoopGroupBuilder(
            String name,
            int childCount,
            EventLoopFactory eventLoopFactory) {
        this.name = requireName(name);
        if (childCount <= 0) {
            throw new IllegalArgumentException("childCount 必须为正数，实际值=" + childCount);
        }
        this.childCount = childCount;
        this.eventLoopFactory = Objects.requireNonNull(
                eventLoopFactory, "eventLoopFactory 不能为空");
    }

    public static EventLoopGroupBuilder bounded(
            String name,
            int childCount,
            int childCapacity) {
        requirePowerOfTwo(childCapacity, "childCapacity");
        return builder(name, childCount, (parent, childIndex) -> EventLoopBuilder
                .bounded(name + "-" + childIndex, childCapacity)
                .build());
    }

    public static EventLoopGroupBuilder unbounded(
            String name,
            int childCount,
            int childSegmentSize) {
        requirePowerOfTwo(childSegmentSize, "childSegmentSize");
        return builder(name, childCount, (parent, childIndex) -> EventLoopBuilder
                .unbounded(name + "-" + childIndex, childSegmentSize)
                .build());
    }

    public static EventLoopGroupBuilder builder(
            String name,
            int childCount,
            EventLoopFactory eventLoopFactory) {
        return new EventLoopGroupBuilder(name, childCount, eventLoopFactory);
    }

    public EventLoopGroupBuilder shutdownTimeout(Duration shutdownTimeout) {
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout 不能为空");
        if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException(
                    "shutdownTimeout 必须为正数，实际值=" + shutdownTimeout);
        }
        shutdownTimeout.toNanos();
        this.shutdownTimeout = shutdownTimeout;
        return this;
    }

    public DisruptorEventLoopGroup build() {
        return new DisruptorEventLoopGroup(
                name, childCount, eventLoopFactory, shutdownTimeout);
    }

    private static String requireName(String value) {
        Objects.requireNonNull(value, "name 不能为空");
        if (value.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        return value;
    }

    private static void requirePowerOfTwo(int value, String field) {
        if (value <= 0 || Integer.bitCount(value) != 1) {
            throw new IllegalArgumentException(
                    field + " 必须为 2 的幂，实际值=" + value);
        }
    }
}
