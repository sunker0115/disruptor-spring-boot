package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;
import com.sstlfsj.disruptor.concurrent.TaskOutcome;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskAdmissionGateTest {

    @Test
    void boundedCapacityCoversAdmissionUntilPhysicalRelease() {
        TaskAdmissionGate gate = TaskAdmissionGate.bounded(2);
        assertNull(gate.tryAcquire());
        gate.open();

        AdmissionToken first = present(gate.tryAcquire());
        AdmissionToken second = present(gate.tryAcquire());
        assertNull(gate.tryAcquire());
        assertEquals(2, gate.outstanding());
        assertEquals(2, gate.activeAdmissions());

        first.commit();
        assertEquals(1, gate.activeAdmissions());
        assertEquals(2, gate.outstanding());
        assertNull(gate.tryAcquire());

        first.releaseOutstanding();
        assertEquals(1, gate.outstanding());
        present(gate.tryAcquire()).abort();
        second.abort();
        assertEquals(0, gate.outstanding());
    }

    @Test
    void closeRejectsNewAdmissionsButLetsExistingTokensFinishBeforeFreeze() throws Exception {
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded();
        gate.open();
        AdmissionToken token = present(gate.tryAcquire());

        gate.closeForAdmissions();

        assertNull(gate.tryAcquire());
        assertFalse(gate.awaitAdmissions(ShutdownDeadline.after(Duration.ofMillis(1))));
        token.commit();
        assertTrue(gate.awaitAdmissions(ShutdownDeadline.unbounded()));
        assertEquals(1, gate.outstanding());
        token.releaseOutstanding();
        assertEquals(0, gate.outstanding());
    }

    @Test
    void committedPermitIsNotReleasedByFutureCancellation() {
        TaskAdmissionGate gate = TaskAdmissionGate.bounded(1);
        gate.open();
        AdmissionToken token = present(gate.tryAcquire());
        token.commit();
        EventLoopFutureTask<Void> future = new EventLoopFutureTask<>(() -> null, () -> 0,
                ScheduledTaskSnapshot.builder()
                        .acceptedSequence(0)
                        .scheduleMode(ScheduleMode.ONE_SHOT)
                        .expiresAtNanos(OptionalLong.empty())
                        .maxExecutions(OptionalInt.empty())
                        .outcome(TaskOutcome.WAITING)
                        .build(), () -> false);
        AcceptedTask<Void> task = new AcceptedTask<>(0, () -> {
        }, future);
        assertTrue(task.tryStart());

        assertEquals(0, gate.remainingCapacity());
        assertNull(gate.tryAcquire());
        assertTrue(future.cancel(CancellationReason.FUTURE_CANCELLED_INTERRUPT));
        assertEquals(0, gate.remainingCapacity());

        token.releaseOutstanding();
        assertEquals(1, gate.remainingCapacity());
    }

    @Test
    void admissionReturnAndFastPhysicalCleanupLinearizeWithoutPermitLeak() throws Exception {
        ExecutorService racers = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < 500; iteration++) {
                TaskAdmissionGate gate = TaskAdmissionGate.bounded(1);
                gate.open();
                AdmissionToken token = present(gate.tryAcquire());
                CountDownLatch start = new CountDownLatch(1);
                Future<?> commit = racers.submit(() -> {
                    start.await();
                    token.commit();
                    return null;
                });
                Future<?> release = racers.submit(() -> {
                    start.await();
                    token.releaseOutstanding();
                    return null;
                });

                start.countDown();
                commit.get(2, TimeUnit.SECONDS);
                release.get(2, TimeUnit.SECONDS);
                assertEquals(0, gate.activeAdmissions());
                assertEquals(0, gate.outstanding());
            }
        } finally {
            racers.shutdownNow();
        }
    }

    private static <T> T present(T value) {
        assertNotNull(value);
        return value;
    }
}
