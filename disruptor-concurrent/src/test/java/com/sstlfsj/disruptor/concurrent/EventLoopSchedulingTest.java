package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopSchedulingTest {

    private static final long DAY_NANOS = Duration.ofDays(1).toNanos();

    @Test
    void supportsJdkOneShotAndPublishesEverySnapshotTransition() throws Exception {
        ManualNanoClock clock = new ManualNanoClock();
        DisruptorEventLoop loop = runningLoop("one-shot", clock);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        EventLoopScheduledFuture<String> future = loop.schedule(ScheduledTaskSpec.<String>builder()
                .task(context -> {
                    entered.countDown();
                    release.await();
                    return "done";
                })
                .triggerAfter(Duration.ofDays(1))
                .build());

        assertEquals(TaskOutcome.WAITING, future.snapshot().outcome());
        assertFalse(future.snapshot().started());
        clock.set(DAY_NANOS);
        wake(loop);
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        assertEquals(TaskOutcome.RUNNING, future.snapshot().outcome());
        assertTrue(future.snapshot().started());
        assertEquals(0, future.snapshot().executions());

        release.countDown();
        assertEquals("done", future.get(2, TimeUnit.SECONDS));
        assertEquals(TaskOutcome.SUCCEEDED, future.snapshot().outcome());
        assertEquals(1, future.snapshot().executions());
        terminate(loop);
    }

    @Test
    void fixedRateCatchesUpWhileFixedDelayUsesRealCompletionTime() throws Exception {
        ManualNanoClock rateClock = new ManualNanoClock();
        DisruptorEventLoop rateLoop = runningLoop("fixed-rate", rateClock);
        EventLoopScheduledFuture<Void> rate = rateLoop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .triggerAfter(Duration.ofDays(1))
                .scheduleMode(ScheduleMode.FIXED_RATE)
                .period(Duration.ofDays(1))
                .maxExecutions(3)
                .build());
        rateClock.set(4 * DAY_NANOS);
        wake(rateLoop);
        awaitCondition(rate::isDone);
        assertEquals(3, rate.snapshot().executions());
        assertEquals(CancellationReason.MAX_EXECUTIONS, rate.snapshot().cancellationReason());
        terminate(rateLoop);

        ManualNanoClock delayClock = new ManualNanoClock();
        DisruptorEventLoop delayLoop = runningLoop("fixed-delay", delayClock);
        EventLoopScheduledFuture<Void> delay = delayLoop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    delayClock.set(DAY_NANOS);
                    return null;
                })
                .scheduleMode(ScheduleMode.FIXED_DELAY)
                .period(Duration.ofDays(1))
                .build());
        awaitCondition(() -> delay.snapshot().executions() == 1);
        assertEquals(2 * DAY_NANOS, delay.snapshot().triggerNanos());
        delay.cancel(false);
        terminate(delayLoop);
    }

    @Test
    void dynamicDelayReceivesLastRunAndFailureCanContinueUntilCountLimit() throws Exception {
        ManualNanoClock clock = new ManualNanoClock();
        AtomicInteger invocations = new AtomicInteger();
        List<Throwable> handled = new ArrayList<>();
        IllegalStateException firstFailure = new IllegalStateException("first");
        DisruptorEventLoop loop = EventLoopBuilder.bounded("dynamic", 16)
                .clock(clock)
                .taskExceptionHandler((eventLoop, command, failure) -> handled.add(failure))
                .build();
        start(loop);
        EventLoopScheduledFuture<Void> future = loop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    if (invocations.getAndIncrement() == 0) {
                        throw firstFailure;
                    }
                    return null;
                })
                .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                .dynamicDelay(lastRun -> {
                    assertTrue(lastRun.executions() >= 1);
                    return Duration.ZERO;
                })
                .continueOnFailure(true)
                .maxExecutions(2)
                .build());

        awaitCondition(future::isDone);
        assertEquals(2, future.snapshot().executions());
        assertEquals(CancellationReason.MAX_EXECUTIONS, future.snapshot().cancellationReason());
        assertEquals(List.of(firstFailure), handled);
        terminate(loop);
    }

    @Test
    void expiresBeforeFirstInvocationAndTokenCancellationPhysicallyClearsTimer() throws Exception {
        ManualNanoClock clock = new ManualNanoClock();
        DisruptorEventLoop loop = runningLoop("cancel-timer", clock);
        AtomicInteger calls = new AtomicInteger();
        EventLoopScheduledFuture<Void> expired = loop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    calls.incrementAndGet();
                    return null;
                })
                .triggerAfter(Duration.ofDays(2))
                .expiresAfter(Duration.ofDays(1))
                .build());
        awaitCondition(() -> loop.snapshot().scheduledPendingTasks() == 1);
        clock.set(2 * DAY_NANOS);
        wake(loop);
        awaitCondition(expired::isDone);
        assertEquals(CancellationReason.EXPIRED, expired.snapshot().cancellationReason());
        assertEquals(0, calls.get());

        CancellationSource source = new CancellationSource();
        EventLoopScheduledFuture<Void> cancelled = loop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .triggerAfter(Duration.ofDays(3))
                .cancellationToken(source)
                .build());
        awaitCondition(() -> loop.snapshot().scheduledPendingTasks() == 1);
        CancellationReason ownerStopped = CancellationReason.of("owner-stopped");
        assertTrue(source.cancel(ownerStopped));
        awaitCondition(() -> loop.snapshot().outstandingTasks() == 0);
        assertSame(ownerStopped, cancelled.snapshot().cancellationReason());
        assertEquals(0, loop.snapshot().scheduledPendingTasks());
        terminate(loop);
    }

    @Test
    void sameTriggerUsesPriorityThenAcceptedSequence() throws Exception {
        ManualNanoClock clock = new ManualNanoClock();
        DisruptorEventLoop loop = runningLoop("priority", clock);
        List<String> order = new ArrayList<>();
        CountDownLatch completed = new CountDownLatch(3);
        scheduleNamed(loop, order, completed, "low", -1);
        scheduleNamed(loop, order, completed, "high-first", 10);
        scheduleNamed(loop, order, completed, "high-second", 10);
        awaitCondition(() -> loop.snapshot().scheduledPendingTasks() == 3);

        clock.set(DAY_NANOS);
        wake(loop);

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        assertEquals(List.of("high-first", "high-second", "low"), order);
        terminate(loop);
    }

    @Test
    void zeroDelayScheduleAcceptedBeforeExecuteAlwaysRunsFirst() throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder.bounded("zero-order", 8).build();
        start(loop);
        List<String> order = new ArrayList<>();
        CountDownLatch completed = new CountDownLatch(2);

        loop.schedule(() -> {
            order.add("timer");
            completed.countDown();
        }, 0, TimeUnit.NANOSECONDS);
        loop.execute(() -> {
            order.add("command");
            completed.countDown();
        });

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        assertEquals(List.of("timer", "command"), order);
        terminate(loop);
    }

    @Test
    void boundedBatchesPreventTimersAndCommandsFromStarvingEachOther() throws Exception {
        ManualNanoClock clock = new ManualNanoClock();
        DisruptorEventLoop loop = EventLoopBuilder.bounded("fairness", 64)
                .clock(clock)
                .maxCommandBatchSize(2)
                .maxTimerBatchSize(2)
                .build();
        start(loop);
        List<String> events = new ArrayList<>();
        EventLoopScheduledFuture<Void> periodic = loop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    events.add("timer");
                    return null;
                })
                .triggerAfter(Duration.ofDays(1))
                .scheduleMode(ScheduleMode.FIXED_RATE)
                .period(Duration.ofDays(1))
                .maxExecutions(20)
                .build());
        awaitCondition(() -> loop.snapshot().scheduledPendingTasks() == 1);
        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        loop.execute(() -> {
            blockerEntered.countDown();
            await(releaseBlocker);
        });
        assertTrue(blockerEntered.await(2, TimeUnit.SECONDS));
        CountDownLatch commands = new CountDownLatch(4);
        for (int index = 1; index <= 4; index++) {
            int commandIndex = index;
            loop.execute(() -> {
                events.add("command-" + commandIndex);
                commands.countDown();
            });
        }
        clock.set(100 * DAY_NANOS);
        releaseBlocker.countDown();

        assertTrue(commands.await(2, TimeUnit.SECONDS));
        int firstCommand = events.indexOf("command-1");
        assertTrue(firstCommand >= 0 && firstCommand <= 2,
                "到期 timer 最多执行一个 timer batch 后必须让出入口");
        periodic.cancel(false);

        ManualNanoClock secondClock = new ManualNanoClock();
        DisruptorEventLoop second = EventLoopBuilder.bounded("fairness-command", 64)
                .clock(secondClock)
                .maxCommandBatchSize(2)
                .maxTimerBatchSize(2)
                .build();
        start(second);
        List<String> secondEvents = new ArrayList<>();
        CountDownLatch timerRan = new CountDownLatch(1);
        second.schedule(() -> {
            secondEvents.add("timer");
            timerRan.countDown();
        }, 1, TimeUnit.DAYS);
        awaitCondition(() -> second.snapshot().scheduledPendingTasks() == 1);
        CountDownLatch secondBlockerEntered = new CountDownLatch(1);
        CountDownLatch releaseSecondBlocker = new CountDownLatch(1);
        second.execute(() -> {
            secondBlockerEntered.countDown();
            await(releaseSecondBlocker);
        });
        assertTrue(secondBlockerEntered.await(2, TimeUnit.SECONDS));
        for (int index = 1; index <= 4; index++) {
            int commandIndex = index;
            second.execute(() -> secondEvents.add("command-" + commandIndex));
        }
        secondClock.set(DAY_NANOS);
        releaseSecondBlocker.countDown();

        assertTrue(timerRan.await(2, TimeUnit.SECONDS));
        assertTrue(secondEvents.indexOf("timer") <= 1,
                "到期 timer 最多被一个 command batch 延后");
        terminate(loop);
        terminate(second);
    }

    @Test
    void cancelInterruptsRunningScheduledInvocationAndCleansAConcurrentRequeue()
            throws Exception {
        DisruptorEventLoop interruptLoop = EventLoopBuilder.bounded("scheduled-interrupt", 8)
                .build();
        start(interruptLoop);
        CountDownLatch invocationEntered = new CountDownLatch(1);
        CountDownLatch invocationInterrupted = new CountDownLatch(1);
        EventLoopScheduledFuture<Void> interrupting = interruptLoop.schedule(
                ScheduledTaskSpec.<Void>builder()
                        .task(context -> {
                            invocationEntered.countDown();
                            try {
                                new CountDownLatch(1).await();
                            } catch (InterruptedException expected) {
                                invocationInterrupted.countDown();
                            }
                            return null;
                        })
                        .build());
        assertTrue(invocationEntered.await(2, TimeUnit.SECONDS));

        assertTrue(interrupting.cancel(true));

        assertTrue(invocationInterrupted.await(2, TimeUnit.SECONDS));
        awaitCondition(() -> interruptLoop.snapshot().outstandingTasks() == 0);
        terminate(interruptLoop);

        DisruptorEventLoop requeueLoop = EventLoopBuilder.bounded("cancel-requeue", 8).build();
        start(requeueLoop);
        CountDownLatch delayEntered = new CountDownLatch(1);
        CountDownLatch releaseDelay = new CountDownLatch(1);
        EventLoopScheduledFuture<Void> requeue = requeueLoop.schedule(
                ScheduledTaskSpec.<Void>builder()
                        .task(context -> null)
                        .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                        .dynamicDelay(lastRun -> {
                            delayEntered.countDown();
                            releaseDelay.await();
                            return Duration.ofDays(1);
                        })
                        .build());
        assertTrue(delayEntered.await(2, TimeUnit.SECONDS));

        assertTrue(requeue.cancel(false));
        releaseDelay.countDown();

        awaitCondition(() -> requeueLoop.snapshot().outstandingTasks() == 0);
        assertEquals(0, requeueLoop.snapshot().scheduledPendingTasks());
        terminate(requeueLoop);
    }

    private static void scheduleNamed(
            DisruptorEventLoop loop,
            List<String> order,
            CountDownLatch completed,
            String name,
            int priority) {
        loop.schedule(ScheduledTaskSpec.<Void>builder()
                .task(context -> {
                    order.add(name);
                    completed.countDown();
                    return null;
                })
                .triggerAfter(Duration.ofDays(1))
                .priority(priority)
                .build());
    }

    private static DisruptorEventLoop runningLoop(String name, NanoClock clock) throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder.bounded(name, 32)
                .clock(clock)
                .build();
        start(loop);
        return loop;
    }

    private static void wake(DisruptorEventLoop loop) {
        loop.execute(() -> { });
    }

    private static void start(DisruptorEventLoop loop) throws Exception {
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static void terminate(DisruptorEventLoop loop) throws Exception {
        loop.shutdownNow();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
        }
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("等待条件超时");
            }
            Thread.sleep(1);
        }
    }

    private static final class ManualNanoClock implements NanoClock {
        private final AtomicLong now = new AtomicLong();

        @Override
        public long nanoTime() {
            return now.get();
        }

        private void set(long value) {
            now.set(value);
        }
    }
}
