package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.NanoClock;
import com.sstlfsj.disruptor.concurrent.ScheduleMode;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSpec;
import com.sstlfsj.disruptor.concurrent.TaskOutcome;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AcceptedTaskRegistryTest {

    @Test
    void waitingTaskCanBeReturnedOrDiscardedButNeverBoth() {
        AcceptedTask<Void> returned = task(1, new NamedRunnable("returned"));
        assertTrue(returned.tryReturn());
        assertFalse(returned.tryDiscard());
        assertEquals(AcceptedTask.PhysicalState.RETURNED, returned.state());

        AcceptedTask<Void> discarded = task(2, new NamedRunnable("discarded"));
        assertTrue(discarded.tryDiscard());
        assertFalse(discarded.tryReturn());
        assertEquals(AcceptedTask.PhysicalState.DISCARDED, discarded.state());
    }

    @Test
    void shutdownScanVisitsTrackedTasksInAdmissionTicketOrder() {
        AcceptedTaskRegistry registry = new AcceptedTaskRegistry();
        registry.register(task(7, new NamedRunnable("seven")));
        registry.register(task(1, new NamedRunnable("one")));
        registry.register(task(5, new NamedRunnable("five")));
        List<Long> visited = new ArrayList<>();

        registry.scanForShutdown(task -> visited.add(task.acceptedSequence()));

        assertEquals(List.of(1L, 5L, 7L), visited);
    }

    @Test
    void shutdownReturnCancelsRunningFutureWithoutAddingItToReturnedSink() {
        AcceptedTaskRegistry registry = new AcceptedTaskRegistry();
        NamedRunnable waitingCommand = new NamedRunnable("waiting");
        NamedRunnable runningCommand = new NamedRunnable("running");
        AcceptedTask<Void> waiting = task(2, waitingCommand);
        AcceptedTask<Void> running = task(1, runningCommand);
        assertTrue(running.tryStart());
        registry.register(waiting);
        registry.register(running);
        List<Runnable> returned = new ArrayList<>();

        registry.scanForShutdown(task -> {
            if (task.tryReturn()) {
                task.future().cancel(CancellationReason.SHUTDOWN_NOW);
                returned.add(task.shutdownNowReturnValue());
            } else if (task.state() == AcceptedTask.PhysicalState.RUNNING) {
                task.future().cancel(true);
            }
        });

        assertEquals(List.of(waitingCommand), returned);
        assertSame(waitingCommand, returned.get(0));
        assertTrue(waiting.future().isCancelled());
        assertTrue(running.future().isCancelled());
        assertEquals(AcceptedTask.PhysicalState.RUNNING, running.state());
    }

    @Test
    void callableSourceUsesItsFutureAsTheSingleShutdownReturnValue() {
        EventLoopFutureTask<Void> future = future(3);
        AcceptedTask<Void> task = AcceptedTask.<Void>builder()
                .acceptedSequence(3)
                .shutdownNowReturnValue(future)
                .future(future)
                .build();

        assertSame(future, task.shutdownNowReturnValue());
    }

    @Test
    void returnToWaitingRequiresTheWholeRearmCheck() {
        AcceptedTask<Void> task = task(4, new NamedRunnable("periodic"));
        assertTrue(task.tryStart());

        assertFalse(task.returnToWaiting(() -> false));
        assertEquals(AcceptedTask.PhysicalState.RUNNING, task.state());
        assertTrue(task.returnToWaiting(() -> true));
        assertEquals(AcceptedTask.PhysicalState.WAITING, task.state());
    }

    @Test
    void nonPeriodicTaskCannotRearm() {
        ManualNanoClock clock = new ManualNanoClock();
        AcceptedTask<Void> task = scheduledTask(10, ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .build(), clock);
        assertTrue(task.tryStart());

        assertFalse(task.returnToWaiting(
                () -> task.scheduledTask().canRearm(true, false)));
    }

    @Test
    void quiescingPeriodicTaskCannotRearm() {
        ManualNanoClock clock = new ManualNanoClock();
        AcceptedTask<Void> task = scheduledTask(11, periodicSpec(), clock);
        assertTrue(task.tryStart());

        assertFalse(task.returnToWaiting(
                () -> task.scheduledTask().canRearm(true, true)));
    }

    @Test
    void expiredPeriodicTaskCannotRearm() {
        ManualNanoClock clock = new ManualNanoClock();
        AcceptedTask<Void> task = scheduledTask(12, periodicSpec().toBuilder()
                .expiresAfter(Duration.ofNanos(1))
                .build(), clock);
        assertTrue(task.tryStart());
        clock.set(1);

        assertFalse(task.returnToWaiting(
                () -> task.scheduledTask().canRearm(true, false)));
    }

    @Test
    void periodicTaskAtMaxExecutionsCannotRearm() {
        ManualNanoClock clock = new ManualNanoClock();
        AcceptedTask<Void> task = scheduledTask(13, periodicSpec().toBuilder()
                .maxExecutions(1)
                .build(), clock);
        assertTrue(task.tryStart());
        boolean invocationRequestedRearm = task.scheduledTask().runInvocation();

        assertFalse(task.returnToWaiting(
                () -> task.scheduledTask().canRearm(invocationRequestedRearm, false)));
    }

    @Test
    void terminalizeAndRemoveIsIdempotentAndNeverThrows() {
        AcceptedTaskRegistry registry = new AcceptedTaskRegistry();
        AcceptedTask<Void> task = task(5, new NamedRunnable("task"));
        registry.register(task);

        registry.terminalizeAndRemove(task);
        registry.terminalizeAndRemove(task);

        assertEquals(AcceptedTask.PhysicalState.TERMINAL, task.state());
        assertEquals(0, registry.size());
    }

    private static AcceptedTask<Void> task(long sequence, NamedRunnable command) {
        return AcceptedTask.<Void>builder()
                .acceptedSequence(sequence)
                .shutdownNowReturnValue(command)
                .future(future(sequence))
                .build();
    }

    private static AcceptedTask<Void> scheduledTask(
            long sequence,
            ScheduledTaskSpec<Void> spec,
            ManualNanoClock clock) {
        ScheduledTask<Void> scheduled = new ScheduledTask<>(sequence, spec, 0, clock);
        AcceptedTask<Void> task = AcceptedTask.<Void>builder()
                .acceptedSequence(sequence)
                .shutdownNowReturnValue(scheduled.future())
                .future(scheduled.future())
                .scheduledTask(scheduled)
                .build();
        scheduled.bind(task);
        return task;
    }

    private static ScheduledTaskSpec<Void> periodicSpec() {
        return ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .scheduleMode(ScheduleMode.FIXED_RATE)
                .period(Duration.ofNanos(1))
                .build();
    }

    private static EventLoopFutureTask<Void> future(long sequence) {
        return new EventLoopFutureTask<>(() -> null, () -> 0,
                ScheduledTaskSnapshot.builder()
                        .acceptedSequence(sequence)
                        .scheduleMode(ScheduleMode.ONE_SHOT)
                        .expiresAtNanos(OptionalLong.empty())
                        .maxExecutions(OptionalInt.empty())
                        .outcome(TaskOutcome.WAITING)
                        .build(), () -> false);
    }

    private record NamedRunnable(String name) implements Runnable {
        @Override
        public void run() {
        }
    }

    private static final class ManualNanoClock implements NanoClock {
        private long now;

        @Override
        public long nanoTime() {
            return now;
        }

        private void set(long now) {
            this.now = now;
        }
    }
}
