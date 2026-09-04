package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import com.sstlfsj.disruptor.core.WorkerSnapshot;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublicContractTest {

    @Test
    void loopAndGroupExposeTheSharedSupervisedSchedulerContract() throws Exception {
        EventLoop loop = null;
        EventLoopGroup group = null;
        SupervisedScheduledExecutor<EventLoopSnapshot> loopExecutor = loop;
        SupervisedScheduledExecutor<EventLoopGroupSnapshot> groupExecutor = group;
        Iterable<EventLoop> children = group;

        assertNull(loopExecutor);
        assertNull(groupExecutor);
        assertNull(children);
        assertTrue(ScheduledExecutorService.class.isAssignableFrom(SupervisedScheduledExecutor.class));
        assertTrue(SupervisedScheduledExecutor.class.isAssignableFrom(EventLoop.class));
        assertTrue(SupervisedScheduledExecutor.class.isAssignableFrom(EventLoopGroup.class));
        assertTrue(Iterable.class.isAssignableFrom(EventLoopGroup.class));

        assertEquals(EventLoopGroup.class, EventLoop.class.getMethod("parent").getReturnType());
        assertEquals(EventLoop.class, EventLoopGroup.class.getMethod("next").getReturnType());
        assertEquals(EventLoop.class,
                EventLoopGroup.class.getMethod("select", int.class).getReturnType());
    }

    @Test
    void factoryAndModulesRemainSmallApplicationFacingContracts() {
        assertTrue(EventLoopFactory.class.isAnnotationPresent(FunctionalInterface.class));
        assertTrue(ContextCallable.class.isAnnotationPresent(FunctionalInterface.class));
        assertTrue(DynamicDelay.class.isAnnotationPresent(FunctionalInterface.class));
        assertTrue(TaskExceptionHandler.class.isAnnotationPresent(FunctionalInterface.class));
        assertTrue(CancellationListenerExceptionHandler.class.isAnnotationPresent(FunctionalInterface.class));
    }

    @Test
    void snapshotsAreValidatedImmutableValues() {
        WorkerSnapshot worker = WorkerSnapshot.builder()
                .name("loop-worker")
                .lifecycle(SupervisedLifecycle.NEW)
                .build();
        EventLoopSnapshot child = EventLoopSnapshot.builder()
                .name("loop")
                .lifecycle(SupervisedLifecycle.NEW)
                .capacityMode(CapacityMode.BOUNDED)
                .capacityLimit(OptionalLong.of(8))
                .worker(worker)
                .build();
        List<EventLoopSnapshot> source = new ArrayList<>(List.of(child));
        EventLoopGroupSnapshot group = EventLoopGroupSnapshot.builder()
                .name("group")
                .lifecycle(SupervisedLifecycle.NEW)
                .childCount(1)
                .children(source)
                .build();
        ScheduledTaskSnapshot task = ScheduledTaskSnapshot.builder()
                .acceptedSequence(1)
                .scheduleMode(ScheduleMode.ONE_SHOT)
                .expiresAtNanos(OptionalLong.empty())
                .maxExecutions(OptionalInt.empty())
                .outcome(TaskOutcome.WAITING)
                .build();

        source.clear();
        assertEquals(List.of(child), group.children());
        assertThrows(UnsupportedOperationException.class, () -> group.children().clear());
        assertEquals(task, task.toBuilder().build());
        assertThrows(IllegalArgumentException.class, () -> EventLoopSnapshot.builder()
                .name("invalid")
                .lifecycle(SupervisedLifecycle.NEW)
                .capacityMode(CapacityMode.UNBOUNDED)
                .capacityLimit(OptionalLong.of(8))
                .worker(worker)
                .build());
    }
}
