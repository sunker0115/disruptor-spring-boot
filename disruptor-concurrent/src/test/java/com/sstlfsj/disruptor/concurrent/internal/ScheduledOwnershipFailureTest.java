package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopSnapshot;
import com.sstlfsj.disruptor.concurrent.NanoClock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScheduledOwnershipFailureTest {

    @ParameterizedTest(name = "{0}, clock read after invocation = {1}")
    @CsvSource({"bounded, 2", "unbounded, 2", "bounded, 3", "unbounded, 3",
            "bounded, 4", "unbounded, 4"})
    void rearmFailureReleasesTheRunningTaskAndAllRetainedOwnership(
            String backend, int failingRead) throws Exception {
        RuntimeException failure = new IllegalStateException("周期重排时钟故障");
        AtomicInteger reads = new AtomicInteger();
        NanoClock clock = () -> {
            if (reads.get() > 0 && reads.getAndIncrement() == failingRead) {
                throw failure;
            }
            return 0;
        };
        EventLoop loop = (backend.equals("bounded")
                ? EventLoopBuilder.bounded("rearm-failure", 8)
                : EventLoopBuilder.unbounded("rearm-failure", 8))
                .clock(clock).build();
        try {
            loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
            List<ScheduledFuture<?>> retained = new ArrayList<>();
            for (int day = 1; day <= 3; day++) {
                retained.add(loop.schedule(() -> { }, day, TimeUnit.DAYS));
            }
            loop.submit(() -> { }).get(2, TimeUnit.SECONDS);
            ScheduledFuture<?> periodic = loop.scheduleAtFixedRate(
                    () -> reads.set(1), 0, 1, TimeUnit.SECONDS);

            EventLoopSnapshot terminated = loop.termination().toCompletableFuture()
                    .get(2, TimeUnit.SECONDS);
            AcceptedTaskRegistry registry = registry(loop);

            assertAll(
                    () -> assertSame(failure, terminated.failure()),
                    () -> assertTrue(periodic.isDone(), "运行中的周期 Future 必须终结"),
                    () -> assertTrue(retained.stream().allMatch(ScheduledFuture::isDone)),
                    () -> assertEquals(0, registry.size(), "tracked index 必须清空"),
                    () -> assertEquals(0, terminated.outstandingTasks()),
                    () -> assertEquals(0, terminated.scheduledPendingTasks()),
                    () -> assertEquals(0, terminated.ingressPendingTasks()),
                    () -> assertEquals(0, terminated.executingTasks()));
        } finally {
            if (!loop.isTerminated()) {
                loop.shutdownNow();
                loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        }
    }

    private static AcceptedTaskRegistry registry(EventLoop loop) throws Exception {
        Field kernelField = loop.getClass().getSuperclass().getDeclaredField("kernel");
        kernelField.setAccessible(true);
        Field registryField = EventLoopKernel.class.getDeclaredField("registry");
        registryField.setAccessible(true);
        return (AcceptedTaskRegistry) registryField.get(kernelField.get(loop));
    }
}
