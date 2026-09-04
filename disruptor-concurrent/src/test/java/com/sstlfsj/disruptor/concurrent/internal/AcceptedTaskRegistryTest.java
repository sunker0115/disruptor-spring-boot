package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;
import com.sstlfsj.disruptor.concurrent.TaskOutcome;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AcceptedTaskRegistryTest {

    @Test
    void shutdownNowReturnsOnlyWaitingOwnedRunnablesInSequenceOrder() {
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded();
        gate.open();
        AcceptedTaskRegistry registry = new AcceptedTaskRegistry();
        NamedRunnable runningCommand = new NamedRunnable("running");
        NamedRunnable cancelledCommand = new NamedRunnable("cancelled");
        NamedRunnable firstWaitingCommand = new NamedRunnable("first-waiting");
        NamedRunnable secondWaitingCommand = new NamedRunnable("second-waiting");
        AcceptedTask<Void> running = register(registry, gate, 1, runningCommand);
        AcceptedTask<Void> cancelled = register(registry, gate, 3, cancelledCommand);
        AcceptedTask<Void> secondWaiting = register(registry, gate, 7, secondWaitingCommand);
        AcceptedTask<Void> firstWaiting = register(registry, gate, 5, firstWaitingCommand);
        assertTrue(running.tryStart());
        assertTrue(registry.cancelWaiting(cancelled, CancellationReason.of("user")));

        List<Runnable> returned = registry.sweepShutdownNow();

        assertEquals(List.of(firstWaitingCommand, secondWaitingCommand), returned);
        org.junit.jupiter.api.Assertions.assertSame(firstWaitingCommand, returned.get(0));
        org.junit.jupiter.api.Assertions.assertSame(secondWaitingCommand, returned.get(1));
        assertEquals(CancellationReason.SHUTDOWN_NOW,
                running.future().snapshot().cancellationReason());
        assertEquals(AcceptedTask.PhysicalState.RUNNING, running.state());
        assertEquals(AcceptedTask.PhysicalState.CANCELLED_WAITING, cancelled.state());
        assertEquals(AcceptedTask.PhysicalState.RETURNED, firstWaiting.state());
        assertEquals(AcceptedTask.PhysicalState.RETURNED, secondWaiting.state());
        assertEquals(List.of(), registry.sweepShutdownNow());
        assertEquals(4, registry.size());
        assertEquals(4, gate.outstanding());

        registry.terminate(running);
        registry.terminate(cancelled);
        registry.terminate(firstWaiting);
        registry.terminate(secondWaiting);
        assertEquals(0, registry.size());
        assertEquals(0, gate.outstanding());
    }

    @Test
    void rollbackAndPhysicalTerminationReleaseExactlyOnce() {
        TaskAdmissionGate gate = TaskAdmissionGate.bounded(1);
        gate.open();
        AcceptedTaskRegistry registry = new AcceptedTaskRegistry();
        AdmissionToken token = gate.tryAcquire();
        AcceptedTask<Void> task = task(0, new NamedRunnable("task"));
        registry.register(task, token);

        assertTrue(registry.rollback(task));
        token.abort();
        assertFalse(registry.rollback(task));
        assertEquals(0, registry.size());
        assertEquals(0, gate.outstanding());

        AcceptedTask<Void> accepted = register(registry, gate, 1, new NamedRunnable("accepted"));
        assertTrue(registry.terminate(accepted));
        assertFalse(registry.terminate(accepted));
        assertEquals(0, gate.outstanding());
    }

    @Test
    void physicalCleanupMayWinAfterPublishBeforeAdmissionReturns() {
        TaskAdmissionGate gate = TaskAdmissionGate.bounded(1);
        gate.open();
        AcceptedTaskRegistry registry = new AcceptedTaskRegistry();
        AdmissionToken token = gate.tryAcquire();
        AcceptedTask<Void> task = task(0, new NamedRunnable("fast"));
        registry.register(task, token);

        assertTrue(registry.terminate(task));
        assertEquals(1, gate.activeAdmissions());
        assertEquals(1, gate.outstanding());

        token.commit();
        assertEquals(0, gate.activeAdmissions());
        assertEquals(0, gate.outstanding());
    }

    private static AcceptedTask<Void> register(
            AcceptedTaskRegistry registry,
            TaskAdmissionGate gate,
            long sequence,
            NamedRunnable command) {
        AdmissionToken token = gate.tryAcquire();
        AcceptedTask<Void> task = task(sequence, command);
        registry.register(task, token);
        token.commit();
        return task;
    }

    private static AcceptedTask<Void> task(long sequence, NamedRunnable command) {
        ScheduledTaskSnapshot snapshot = ScheduledTaskSnapshot.builder()
                .acceptedSequence(sequence)
                .scheduleMode(ScheduleMode.ONE_SHOT)
                .expiresAtNanos(OptionalLong.empty())
                .maxExecutions(OptionalInt.empty())
                .outcome(TaskOutcome.WAITING)
                .build();
        return new AcceptedTask<>(sequence, command,
                new EventLoopFutureTask<>(() -> null, () -> 0, snapshot, () -> false));
    }

    private record NamedRunnable(String name) implements Runnable {
        @Override
        public void run() {
        }
    }
}
