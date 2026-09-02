package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerSupervisorTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    @Test
    void completesGracefulShutdownAfterAllWorkersExit() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<ShutdownMode> stoppedWith = new AtomicReference<>();
        AtomicBoolean stopRanOnVirtualThread = new AtomicBoolean();
        AtomicReference<String> stopThreadName = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> {
            stoppedWith.set(mode);
            stopRanOnVirtualThread.set(Thread.currentThread().isVirtual());
            stopThreadName.set(Thread.currentThread().getName());
            release.countDown();
        });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(release);
        }, "graceful-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);

        WorkerSnapshot result = terminate(supervisor);
        assertEquals(PipelineLifecycle.TERMINATED, result.lifecycle());
        assertEquals(1, result.registeredWorkers());
        assertEquals(0, result.aliveWorkers());
        assertNull(result.failure());
        assertEquals(ShutdownMode.GRACEFUL, stoppedWith.get());
        assertTrue(stopRanOnVirtualThread.get());
        assertEquals("worker-supervisor-workers", stopThreadName.get());
        assertEquals(ShutdownMode.GRACEFUL, result.shutdownMode());
        assertTrue(result.gracefulTermination());
    }

    @Test
    void preservesThrownObjectForFailureAndUncaughtExceptionHandler() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch throwNow = new CountDownLatch(1);
        Exception original = new Exception("boom");
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(throwNow);
            WorkerSupervisorTest.<RuntimeException>throwUnchecked(original);
        }, "failing-worker");
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        throwNow.countDown();

        WorkerSnapshot result = terminate(supervisor);
        assertSame(original, result.failure());
        assertSame(original, uncaught.get());
        assertEquals(ShutdownMode.IMMEDIATE, result.shutdownMode());
        assertFalse(result.gracefulTermination());
    }

    @Test
    void treatsNormalReturnDuringRunningAsUnexpectedFailure() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch returnNow = new CountDownLatch(1);
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(returnNow);
        }, "early-return-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        returnNow.countDown();

        WorkerSnapshot result = terminate(supervisor);
        assertInstanceOf(WorkerSupervisor.UnexpectedWorkerExitException.class, result.failure());
        assertEquals(ShutdownMode.IMMEDIATE, result.shutdownMode());
    }

    @Test
    void treatsNormalReturnDuringStartingAsUnexpectedFailureAndCannotMarkRunning() throws Exception {
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> { }, "starting-return-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        worker.join(TEST_TIMEOUT.toMillis());

        assertFalse(worker.isAlive());
        assertInstanceOf(WorkerSupervisor.UnexpectedWorkerExitException.class,
                supervisor.snapshot().failure());
        assertEquals(ShutdownMode.IMMEDIATE, supervisor.snapshot().shutdownMode());
        assertThrows(IllegalStateException.class, supervisor::markRunning);
        assertEquals(PipelineLifecycle.TERMINATED, terminate(supervisor).lifecycle());
    }

    @Test
    void keepsFirstFailure() {
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        IllegalStateException first = new IllegalStateException("first");

        supervisor.fail(first);
        supervisor.fail(new IllegalArgumentException("second"));

        assertSame(first, terminate(supervisor).failure());
        assertThrows(NullPointerException.class, () -> supervisor.fail(null));
    }

    @Test
    void repeatedShutdownIsIdempotentAndImmediateUpgradesGraceful() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch gracefulApplied = new CountDownLatch(1);
        List<ShutdownMode> appliedModes = new CopyOnWriteArrayList<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> {
            appliedModes.add(mode);
            if (mode == ShutdownMode.GRACEFUL) {
                gracefulApplied.countDown();
            }
        });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }, "upgrade-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(gracefulApplied.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);

        WorkerSnapshot result = terminate(supervisor);
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertEquals(List.of(ShutdownMode.GRACEFUL, ShutdownMode.IMMEDIATE), appliedModes);
        assertEquals(ShutdownMode.IMMEDIATE, result.shutdownMode());
        assertFalse(result.gracefulTermination());
    }

    @Test
    void shutdownRequestedByWorkerReturnsWithoutSelfWait() throws Exception {
        CountDownLatch requestReturned = new CountDownLatch(1);
        AtomicReference<WorkerSupervisor> reference = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        reference.set(supervisor);
        Thread worker = worker(supervisor, () -> {
            reference.get().requestShutdown(ShutdownMode.GRACEFUL);
            requestReturned.countDown();
        }, "self-shutdown-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        supervisor.markRunning();
        worker.start();

        assertTrue(requestReturned.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        WorkerSnapshot result = terminate(supervisor);
        assertNull(result.failure());
        assertTrue(result.gracefulTermination());
    }

    @Test
    void stopActionFailureTriggersImmediateStopAndIsRetained() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        RuntimeException original = new RuntimeException("stop failed");
        AtomicInteger calls = new AtomicInteger();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> {
            calls.incrementAndGet();
            throw original;
        });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }, "stop-failure-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);

        WorkerSnapshot result = terminate(supervisor);
        assertSame(original, result.failure());
        assertEquals(ShutdownMode.IMMEDIATE, result.shutdownMode());
        assertEquals(2, calls.get());
    }

    @Test
    void interruptionDuringStoppingIsNotRecordedAsANewFailure() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException interrupted) {
                WorkerSupervisorTest.<RuntimeException>throwUnchecked(interrupted);
            }
        }, "interrupted-worker");
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);

        WorkerSnapshot result = terminate(supervisor);
        assertNull(result.failure());
        assertInstanceOf(InterruptedException.class, uncaught.get());
        assertFalse(result.gracefulTermination());
    }

    @Test
    void ordinaryThrowableDuringQuiescingIsNotRecordedOrUpgraded() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch gracefulApplied = new CountDownLatch(1);
        CountDownLatch throwNow = new CountDownLatch(1);
        RuntimeException shutdownExit = new RuntimeException("backend halted");
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> {
            if (mode == ShutdownMode.GRACEFUL) {
                gracefulApplied.countDown();
            }
        });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(throwNow);
            throw shutdownExit;
        }, "quiescing-failure-worker");
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(gracefulApplied.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        throwNow.countDown();

        WorkerSnapshot result = terminate(supervisor);
        assertSame(shutdownExit, uncaught.get());
        assertNull(result.failure());
        assertEquals(ShutdownMode.GRACEFUL, result.shutdownMode());
        assertTrue(result.gracefulTermination());
    }

    @Test
    void acceptedShutdownWinsRaceWithWorkerFailureReporting() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch throwNow = new CountDownLatch(1);
        RuntimeException shutdownExit = new RuntimeException("shutdown race");
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(throwNow);
            throw shutdownExit;
        }, "shutdown-race-worker");
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();

        synchronized (stateLock(supervisor)) {
            throwNow.countDown();
            awaitThreadState(worker, Thread.State.BLOCKED);
            supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        }

        WorkerSnapshot result = terminate(supervisor);
        assertSame(shutdownExit, uncaught.get());
        assertNull(result.failure());
        assertEquals(ShutdownMode.GRACEFUL, result.shutdownMode());
        assertTrue(result.gracefulTermination());
    }

    @Test
    void failPublishesFailureAndImmediateTransitionInOneCriticalSection() throws Exception {
        RuntimeException firstFailure = new RuntimeException("late failure");
        CountDownLatch gracefulActionEntered = new CountDownLatch(1);
        AtomicReference<WorkerSupervisor> supervisorReference = new AtomicReference<>();
        AtomicReference<ShutdownMode> modeObservedUnderStateLock = new AtomicReference<>();
        AtomicReference<PipelineLifecycle> lifecycleObservedUnderStateLock = new AtomicReference<>();
        AtomicBoolean controlThreadPrepared = new AtomicBoolean();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> {
            if (mode != ShutdownMode.GRACEFUL) {
                return;
            }
            WorkerSupervisor current = supervisorReference.get();
            gracefulActionEntered.countDown();
            AtomicReference<Throwable> failure = failureReference(current);
            while (failure.get() != firstFailure && System.nanoTime() < deadlineNanos) {
                Thread.onSpinWait();
            }
            synchronized (stateLock(current)) {
                WorkerSnapshot snapshot = current.snapshot();
                modeObservedUnderStateLock.set(snapshot.shutdownMode());
                lifecycleObservedUnderStateLock.set(snapshot.lifecycle());
                controlThreadPrepared.set(controlThread(current) != null);
            }
        });
        supervisorReference.set(supervisor);
        supervisor.markStarting();
        supervisor.register(new Thread(() -> { }, "unstarted-worker"));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(gracefulActionEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        Thread failer = Thread.ofPlatform().name("late-failer")
                .start(() -> supervisor.fail(firstFailure));
        failer.join(TEST_TIMEOUT.toMillis());
        WorkerSnapshot result = terminate(supervisor);

        assertFalse(failer.isAlive());
        assertEquals(ShutdownMode.IMMEDIATE, modeObservedUnderStateLock.get());
        assertEquals(PipelineLifecycle.STOPPING, lifecycleObservedUnderStateLock.get());
        assertTrue(controlThreadPrepared.get());
        assertSame(firstFailure, result.failure());
        assertEquals(ShutdownMode.IMMEDIATE, result.shutdownMode());
    }

    @Test
    void deadlineRecordsTimeoutAndInterruptsRemainingWorker() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger interrupts = new AtomicInteger();
        WorkerSupervisor supervisor = supervisor(1, Duration.ofMillis(50), (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            boolean done = false;
            while (!done) {
                try {
                    release.await();
                    done = true;
                } catch (InterruptedException interrupted) {
                    interrupts.incrementAndGet();
                }
            }
        }, "stubborn-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);

        WorkerSnapshot result = terminate(supervisor);
        assertInstanceOf(WorkerSupervisor.WorkerTerminationTimeoutException.class, result.failure());
        assertEquals(PipelineLifecycle.TERMINATED, result.lifecycle());
        assertEquals(1, result.aliveWorkers());
        assertTrue(interrupts.get() >= 1);
        release.countDown();
        worker.join(TEST_TIMEOUT.toMillis());
        assertFalse(worker.isAlive());
    }

    @Test
    void validatesRegistrationAndLifecycleTransitions() {
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        Thread first = new Thread(() -> { });
        Thread second = new Thread(() -> { });

        assertEquals(PipelineLifecycle.NEW, supervisor.snapshot().lifecycle());
        assertNull(supervisor.snapshot().shutdownMode());
        assertThrows(IllegalStateException.class, supervisor::markRunning);
        supervisor.markStarting();
        assertThrows(IllegalStateException.class, supervisor::markStarting);
        assertThrows(IllegalStateException.class, supervisor::markRunning);
        supervisor.register(first);
        assertThrows(IllegalStateException.class, () -> supervisor.register(first));
        assertThrows(IllegalStateException.class, () -> supervisor.register(second));
        supervisor.markRunning();
        assertThrows(IllegalStateException.class, () -> supervisor.register(second));
        assertThrows(IllegalStateException.class, supervisor::markRunning);
    }

    @Test
    void validatesBuilderAndSnapshotCounts() {
        assertThrows(NullPointerException.class, () -> WorkerSupervisor.builder()
                .expectedWorkers(1).shutdownTimeout(TEST_TIMEOUT).stopAction((mode, deadline) -> { }).build());
        assertThrows(IllegalArgumentException.class, () -> WorkerSupervisor.builder()
                .name(" ").expectedWorkers(1).shutdownTimeout(TEST_TIMEOUT)
                .stopAction((mode, deadline) -> { }).build());
        assertThrows(IllegalArgumentException.class, () -> WorkerSupervisor.builder()
                .name("workers").expectedWorkers(0).shutdownTimeout(TEST_TIMEOUT)
                .stopAction((mode, deadline) -> { }).build());
        assertThrows(IllegalArgumentException.class, () -> WorkerSupervisor.builder()
                .name("workers").expectedWorkers(1).shutdownTimeout(Duration.ZERO)
                .stopAction((mode, deadline) -> { }).build());
        assertThrows(NullPointerException.class, () -> WorkerSupervisor.builder()
                .name("workers").expectedWorkers(1).shutdownTimeout(TEST_TIMEOUT).build());
        assertThrows(IllegalArgumentException.class, () -> new WorkerSnapshot(
                "workers", PipelineLifecycle.RUNNING, 1, 0, 1, null, null, false));
        assertThrows(IllegalArgumentException.class, () -> new WorkerSnapshot(
                "workers", PipelineLifecycle.RUNNING, 1, 2, 0, null, null, false));
        assertThrows(IllegalArgumentException.class, () -> new WorkerSnapshot(
                "workers", PipelineLifecycle.RUNNING, 1, 1, -1, null, null, false));
    }

    @Test
    void terminationStageCannotCompleteSupervisor() {
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        CompletionStage<WorkerSnapshot> stage = supervisor.termination();
        WorkerSnapshot forged = new WorkerSnapshot("forged", PipelineLifecycle.TERMINATED,
                1, 0, 0, null, ShutdownMode.GRACEFUL, true);

        assertTrue(stage.toCompletableFuture().complete(forged));
        assertEquals(PipelineLifecycle.NEW, supervisor.snapshot().lifecycle());
        assertFalse(supervisor.termination().toCompletableFuture().isDone());

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertEquals("workers", terminate(supervisor).name());
    }

    @Test
    void failAfterGracefulTerminationDoesNotChangeSnapshotsOrTermination() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT,
                (mode, deadlineNanos) -> release.countDown());
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(release);
        }, "terminated-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        WorkerSnapshot terminated = terminate(supervisor);

        supervisor.fail(new IllegalStateException("too late"));

        assertEquals(terminated, supervisor.snapshot());
        assertEquals(terminated, terminate(supervisor));
        assertNull(supervisor.snapshot().failure());
        assertEquals(ShutdownMode.GRACEFUL, supervisor.snapshot().shutdownMode());
        assertTrue(supervisor.snapshot().gracefulTermination());
    }

    private static WorkerSupervisor supervisor(
            int expectedWorkers,
            Duration shutdownTimeout,
            WorkerSupervisor.StopAction stopAction) {
        return WorkerSupervisor.builder()
                .name("workers")
                .expectedWorkers(expectedWorkers)
                .shutdownTimeout(shutdownTimeout)
                .stopAction(stopAction)
                .build();
    }

    private static Thread worker(WorkerSupervisor supervisor, Runnable task, String name) {
        return Thread.ofPlatform().name(name).unstarted(supervisor.supervise(task));
    }

    private static WorkerSnapshot terminate(WorkerSupervisor supervisor) {
        try {
            return supervisor.termination().toCompletableFuture()
                    .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception failure) {
            throw new AssertionError("等待 supervisor 终止失败", failure);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("等待 latch 超时");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("等待 latch 被中断", interrupted);
        }
    }

    private static void awaitThreadState(Thread thread, Thread.State expected) {
        long deadline = System.nanoTime() + TEST_TIMEOUT.toNanos();
        while (thread.getState() != expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(expected, thread.getState());
    }

    private static Object stateLock(WorkerSupervisor supervisor) {
        return fieldValue(supervisor, "stateLock", Object.class);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Throwable> failureReference(WorkerSupervisor supervisor) {
        return fieldValue(supervisor, "failure", AtomicReference.class);
    }

    private static Thread controlThread(WorkerSupervisor supervisor) {
        return fieldValue(supervisor, "controlThread", Thread.class);
    }

    private static <T> T fieldValue(WorkerSupervisor supervisor, String name, Class<T> type) {
        try {
            Field field = WorkerSupervisor.class.getDeclaredField(name);
            field.setAccessible(true);
            return type.cast(field.get(supervisor));
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("读取测试同步字段失败：" + name, failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }
}
