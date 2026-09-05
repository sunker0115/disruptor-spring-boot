package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.DynamicDelay;
import com.sstlfsj.disruptor.concurrent.CancellationReason;
import com.sstlfsj.disruptor.concurrent.CancellationSource;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScheduledTaskTest {

    @Test
    void computesFixedRateFromLogicalTriggerAndFixedDelayFromRealCompletion() {
        ManualNanoClock clock = new ManualNanoClock(100);
        ScheduledTask<Void> fixedRate = new ScheduledTask<>(1,
                periodicSpec(ScheduleMode.FIXED_RATE, context -> null), clock.nanoTime(), clock);
        clock.set(120);

        assertTrue(fixedRate.runInvocation());
        assertEquals(120, fixedRate.triggerNanos());
        assertEquals(1, fixedRate.future().snapshot().executions());

        ScheduledTask<Void> fixedDelay = new ScheduledTask<>(2,
                periodicSpec(ScheduleMode.FIXED_DELAY, context -> {
                    clock.set(150);
                    return null;
                }), 100, clock);
        clock.set(120);

        assertTrue(fixedDelay.runInvocation());
        assertEquals(160, fixedDelay.triggerNanos());
    }

    @Test
    void dynamicDelayUsesLastRunSnapshot() {
        ManualNanoClock clock = new ManualNanoClock(5);
        ScheduledTask<Void> task = new ScheduledTask<>(3, ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                .dynamicDelay(lastRun -> {
                    assertEquals(1, lastRun.executions());
                    assertEquals(TaskOutcome.WAITING, lastRun.outcome());
                    return Duration.ofNanos(7);
                })
                .build(), clock.nanoTime(), clock);

        assertTrue(task.runInvocation());
        assertEquals(12, task.triggerNanos());
    }

    @Test
    void reschedulePublishesExecutionAndNextTriggerInOneSnapshot() {
        ManualNanoClock clock = new ManualNanoClock(5);
        AtomicReference<ScheduledTask<Void>> taskReference = new AtomicReference<>();
        AtomicReference<ScheduledTaskSnapshot> duringReschedule = new AtomicReference<>();
        ScheduledTask<Void> task = new ScheduledTask<>(4, ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                .dynamicDelay(lastRun -> {
                    duringReschedule.set(taskReference.get().future().snapshot());
                    return Duration.ofNanos(7);
                })
                .build(), clock.nanoTime(), clock);
        taskReference.set(task);

        assertTrue(task.runInvocation());

        assertEquals(TaskOutcome.RUNNING, duringReschedule.get().outcome());
        assertEquals(0, duringReschedule.get().executions());
        assertEquals(TaskOutcome.WAITING, task.future().snapshot().outcome());
        assertEquals(1, task.future().snapshot().executions());
        assertEquals(12, task.future().snapshot().triggerNanos());
    }

    @Test
    void invalidDynamicDelayFailsFuture() {
        assertDynamicFailure(lastRun -> null, NullPointerException.class);
        assertDynamicFailure(lastRun -> Duration.ofNanos(-1), IllegalArgumentException.class);
    }

    @Test
    void maxExecutionsCancelsPeriodicFutureWithExplicitReason() {
        ManualNanoClock clock = new ManualNanoClock(0);
        ScheduledTask<Void> task = new ScheduledTask<>(4, ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .scheduleMode(ScheduleMode.FIXED_RATE)
                .period(Duration.ofNanos(1))
                .maxExecutions(1)
                .build(), clock.nanoTime(), clock);

        assertFalse(task.runInvocation());
        assertTrue(task.future().isCancelled());
        assertEquals(TaskOutcome.CANCELLED, task.future().snapshot().outcome());
        assertEquals("max-executions",
                task.future().snapshot().cancellationReason().code());
    }

    @Test
    void tokenCancellationUsesTheFrozenReasonAndUnlinksRegistration() {
        ManualNanoClock clock = new ManualNanoClock(0);
        CancellationSource source = new CancellationSource();
        ScheduledTask<Void> task = new ScheduledTask<>(5, ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .cancellationToken(source)
                .build(), clock.nanoTime(), clock);
        CancellationReason reason = CancellationReason.interrupting("owner-stopped");

        assertTrue(source.cancel(reason));

        assertFalse(task.runInvocation());
        assertTrue(task.future().isCancelled());
        assertEquals(reason, task.future().snapshot().cancellationReason());
    }

    @Test
    void continuedFailureIsReportedAndPeriodicTaskIsRescheduled() {
        ManualNanoClock clock = new ManualNanoClock(0);
        IllegalStateException failure = new IllegalStateException("retry");
        List<Throwable> reported = new ArrayList<>();
        ScheduledTask<Void> task = new ScheduledTask<>(6, ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    throw failure;
                })
                .scheduleMode(ScheduleMode.FIXED_DELAY)
                .period(Duration.ofNanos(3))
                .continueOnFailure(true)
                .build(), clock.nanoTime(), clock, reported::add);

        assertTrue(task.runInvocation());

        assertEquals(List.of(failure), reported);
        assertFalse(task.future().isDone());
        assertEquals(failure, task.future().snapshot().lastFailure());
        assertEquals(3, task.triggerNanos());
    }

    @Test
    void ordinaryFutureRunPublishesAConsistentTerminalSnapshot() throws Exception {
        ManualNanoClock clock = new ManualNanoClock(0);
        ScheduledTaskSnapshot initial = ScheduledTaskSnapshot.builder()
                .acceptedSequence(7)
                .scheduleMode(ScheduleMode.ONE_SHOT)
                .expiresAtNanos(OptionalLong.empty())
                .maxExecutions(OptionalInt.empty())
                .outcome(TaskOutcome.WAITING)
                .build();
        EventLoopFutureTask<String> future = new EventLoopFutureTask<>(
                () -> "value", clock, initial, () -> false);

        future.run();

        assertEquals("value", future.get());
        assertEquals(TaskOutcome.SUCCEEDED, future.snapshot().outcome());
        assertEquals(1, future.snapshot().executions());
        assertTrue(future.snapshot().started());
    }

    @Test
    void acceptedTaskSeparatesFutureOutcomeFromPhysicalCleanup() {
        EventLoopFutureTask<Void> future = future(8);
        AcceptedTask<Void> accepted = new AcceptedTask<>(8, () -> {
        }, future);

        assertTrue(accepted.tryStart());
        assertTrue(future.cancel(CancellationReason.FUTURE_CANCELLED_INTERRUPT));
        assertEquals(AcceptedTask.PhysicalState.RUNNING, accepted.state());

        assertTrue(accepted.terminate());
        assertFalse(accepted.terminate());
        assertEquals(AcceptedTask.PhysicalState.TERMINAL, accepted.state());
    }

    @Test
    void cancellationMailboxKeepsAtMostOnePendingNodePerTask() {
        AcceptedTask<Void> accepted = new AcceptedTask<>(9, () -> {
        }, future(9));
        CancellationMailbox mailbox = new CancellationMailbox();

        assertTrue(mailbox.offer(accepted));
        assertFalse(mailbox.offer(accepted));
        assertEquals(accepted, mailbox.poll());
        assertTrue(mailbox.isEmpty());
    }

    private static ScheduledTaskSpec<Void> periodicSpec(
            ScheduleMode mode,
            com.sstlfsj.disruptor.concurrent.ContextCallable<Void> callable) {
        return ScheduledTaskSpec.<Void>builder()
                .task(callable)
                .scheduleMode(mode)
                .triggerAfter(Duration.ofNanos(10))
                .period(Duration.ofNanos(10))
                .build();
    }

    private static void assertDynamicFailure(
            DynamicDelay dynamicDelay,
            Class<? extends Throwable> expected) {
        ManualNanoClock clock = new ManualNanoClock(0);
        ScheduledTask<Void> task = new ScheduledTask<>(0, ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                .dynamicDelay(dynamicDelay)
                .build(), clock.nanoTime(), clock);

        assertFalse(task.runInvocation());
        ExecutionException thrown = assertThrows(ExecutionException.class, task.future()::get);
        assertInstanceOf(expected, thrown.getCause());
        assertEquals(TaskOutcome.FAILED, task.future().snapshot().outcome());
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

    private static final class ManualNanoClock implements NanoClock {
        private long now;

        private ManualNanoClock(long now) {
            this.now = now;
        }

        @Override
        public long nanoTime() {
            return now;
        }

        private void set(long now) {
            this.now = now;
        }
    }
}
