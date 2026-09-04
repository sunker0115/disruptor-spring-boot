package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;
import com.sstlfsj.disruptor.concurrent.TaskOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskQueueContractTest {

    private static final int PRODUCERS = 4;
    private static final int TASKS_PER_PRODUCER = 10_000;

    static Stream<QueueFactory> queues() {
        return Stream.of(
                new QueueFactory("bounded", () -> new BoundedTaskQueue(65_536)),
                new QueueFactory("unbounded", () -> new UnboundedTaskQueue(1_024)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void concurrentReservationsPublishEverySequenceExactlyOnce(QueueFactory factory) throws Exception {
        TaskQueue queue = factory.create();
        ExecutorService producers = Executors.newFixedThreadPool(PRODUCERS + 1);
        CountDownLatch start = new CountDownLatch(1);
        try {
            int total = PRODUCERS * TASKS_PER_PRODUCER;
            Future<?> consumption = producers.submit(() -> {
                start.await();
                long expectedSequence = 0;
                while (expectedSequence < total) {
                    AcceptedTask<?> task = queue.poll();
                    if (task == null) {
                        Thread.onSpinWait();
                        continue;
                    }
                    assertEquals(expectedSequence, task.acceptedSequence());
                    expectedSequence++;
                }
                return null;
            });
            List<Future<?>> writes = new ArrayList<>();
            for (int producer = 0; producer < PRODUCERS; producer++) {
                writes.add(producers.submit(() -> {
                    start.await();
                    for (int index = 0; index < TASKS_PER_PRODUCER; index++) {
                        TaskReservation reservation = present(queue.tryReserve());
                        reservation.publish(task(reservation.sequence()));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> write : writes) {
                write.get(10, TimeUnit.SECONDS);
            }
            consumption.get(10, TimeUnit.SECONDS);

            assertNull(queue.poll());
            assertEquals(0, queue.pending());
            assertEquals(0, retainedReferences(queue));
        } finally {
            producers.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void abortPublishesTombstoneWithoutLeavingASequenceHole(QueueFactory factory) {
        TaskQueue queue = factory.create();
        TaskReservation first = present(queue.tryReserve());
        TaskReservation second = present(queue.tryReserve());
        second.publish(task(second.sequence()));

        assertNull(queue.poll());
        first.abort();

        assertEquals(second.sequence(), present(queue.poll()).acceptedSequence());
        assertNull(queue.poll());
        assertEquals(0, queue.pending());
        assertEquals(0, retainedReferences(queue));
    }

    @Test
    void boundedQueueDoesNotReuseASlotBeforeSingleConsumerAdvances() {
        BoundedTaskQueue queue = new BoundedTaskQueue(2);
        TaskReservation first = present(queue.tryReserve());
        TaskReservation second = present(queue.tryReserve());
        assertNull(queue.tryReserve());
        first.publish(task(first.sequence()));
        second.publish(task(second.sequence()));
        assertNull(queue.tryReserve());

        assertNotNull(queue.poll());

        present(queue.tryReserve()).abort();
        assertNotNull(queue.poll());
        assertNull(queue.poll());
        assertEquals(0, queue.retainedReferences());
    }

    @Test
    void unboundedQueueReclaimsConsumedSegments() {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(4);
        for (int index = 0; index < 12; index++) {
            TaskReservation reservation = queue.tryReserve();
            reservation.publish(task(reservation.sequence()));
        }
        assertEquals(3, queue.allocatedSegments());

        for (int index = 0; index < 12; index++) {
            assertNotNull(queue.poll());
        }

        assertEquals(1, queue.allocatedSegments());
        assertEquals(0, queue.retainedReferences());
    }

    @Test
    void unboundedQueueAdvancesWhenNextSegmentIsAllocatedAfterBoundaryConsumption() {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(2);
        for (int index = 0; index < 2; index++) {
            TaskReservation reservation = queue.tryReserve();
            reservation.publish(task(reservation.sequence()));
        }
        assertNotNull(queue.poll());
        assertNotNull(queue.poll());

        TaskReservation later = queue.tryReserve();
        later.publish(task(later.sequence()));

        assertEquals(later.sequence(), present(queue.poll()).acceptedSequence());
        assertEquals(1, queue.allocatedSegments());
    }

    private static AcceptedTask<Void> task(long sequence) {
        ScheduledTaskSnapshot snapshot = ScheduledTaskSnapshot.builder()
                .acceptedSequence(sequence)
                .scheduleMode(ScheduleMode.ONE_SHOT)
                .expiresAtNanos(OptionalLong.empty())
                .maxExecutions(OptionalInt.empty())
                .outcome(TaskOutcome.WAITING)
                .build();
        return new AcceptedTask<>(sequence, () -> {
        }, new EventLoopFutureTask<>(() -> null, () -> 0, snapshot, () -> false));
    }

    private static int retainedReferences(TaskQueue queue) {
        if (queue instanceof BoundedTaskQueue bounded) {
            return bounded.retainedReferences();
        }
        return ((UnboundedTaskQueue) queue).retainedReferences();
    }

    private static <T> T present(T value) {
        assertNotNull(value);
        return value;
    }

    private record QueueFactory(String name, Factory factory) {
        TaskQueue create() {
            return factory.create();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @FunctionalInterface
    private interface Factory {
        TaskQueue create();
    }
}
