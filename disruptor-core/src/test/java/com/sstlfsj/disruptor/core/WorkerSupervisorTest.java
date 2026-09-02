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
        assertTrue(result.failure().getMessage().contains("RUNNING"));
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
        assertTrue(supervisor.snapshot().failure().getMessage().contains("STARTING"));
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
    void immediateUpgradeInterruptsWorkerWhileGracefulActionIsStillRunning() throws Exception {
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        CountDownLatch gracefulActionEntered = new CountDownLatch(1);
        CountDownLatch allowGracefulActionToReturn = new CountDownLatch(1);
        List<ShutdownMode> appliedModes = new CopyOnWriteArrayList<>();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> {
            appliedModes.add(mode);
            if (mode == ShutdownMode.GRACEFUL) {
                gracefulActionEntered.countDown();
                await(allowGracefulActionToReturn);
            }
        });
        Thread worker = worker(supervisor, () -> {
            workerEntered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException interrupted) {
                workerInterrupted.countDown();
                Thread.currentThread().interrupt();
            }
        }, "blocked-graceful-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(workerEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(gracefulActionEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(1L, workerInterrupted.getCount());

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        boolean interruptedBeforeGracefulActionReturned =
                workerInterrupted.await(200, TimeUnit.MILLISECONDS);
        List<ShutdownMode> modesBeforeGracefulActionReturned = List.copyOf(appliedModes);
        allowGracefulActionToReturn.countDown();

        assertTrue(interruptedBeforeGracefulActionReturned);
        assertEquals(List.of(ShutdownMode.GRACEFUL), modesBeforeGracefulActionReturned);
        assertEquals(ShutdownMode.IMMEDIATE, terminate(supervisor).shutdownMode());
        assertEquals(List.of(ShutdownMode.GRACEFUL, ShutdownMode.IMMEDIATE), appliedModes);
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
            while (current.snapshot().failure() != firstFailure && System.nanoTime() < deadlineNanos) {
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
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        WorkerSupervisor supervisor = supervisor(1, Duration.ofMillis(50), (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            boolean done = false;
            while (!done) {
                try {
                    release.await();
                    done = true;
                } catch (InterruptedException interrupted) {
                    workerInterrupted.countDown();
                }
            }
        }, "stubborn-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        WorkerSnapshot stopping;
        try {
            assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            supervisor.markRunning();
            supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
            stopping = awaitFailure(supervisor);
            assertInstanceOf(WorkerSupervisor.WorkerTerminationTimeoutException.class, stopping.failure());
            assertEquals(PipelineLifecycle.STOPPING, stopping.lifecycle());
            assertEquals(1, stopping.aliveWorkers());
            assertFalse(supervisor.termination().toCompletableFuture().isDone());
            assertTrue(workerInterrupted.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        } finally {
            release.countDown();
            worker.join(TEST_TIMEOUT.toMillis());
        }

        WorkerSnapshot result = terminate(supervisor);
        assertEquals(PipelineLifecycle.TERMINATED, result.lifecycle());
        assertEquals(0, result.aliveWorkers());
        assertSame(stopping.failure(), result.failure());
    }

    @Test
    void gracefulTimeoutStillAppliesImmediateStopActionBeforeTermination() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch immediateApplied = new CountDownLatch(1);
        List<ShutdownMode> appliedModes = new CopyOnWriteArrayList<>();
        WorkerSupervisor supervisor = supervisor(1, Duration.ofMillis(50), (mode, deadlineNanos) -> {
            appliedModes.add(mode);
            if (mode == ShutdownMode.IMMEDIATE) {
                immediateApplied.countDown();
            }
        });
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            boolean done = false;
            while (!done) {
                try {
                    release.await();
                    done = true;
                } catch (InterruptedException ignored) {
                    // 故意保持存活，让 GRACEFUL 等待触发绝对截止时间。
                }
            }
        }, "graceful-timeout-worker");

        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        WorkerSnapshot stopping;
        try {
            assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            supervisor.markRunning();
            supervisor.requestShutdown(ShutdownMode.GRACEFUL);
            stopping = awaitFailure(supervisor);
            assertTrue(immediateApplied.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            assertInstanceOf(WorkerSupervisor.WorkerTerminationTimeoutException.class, stopping.failure());
            assertEquals(PipelineLifecycle.STOPPING, stopping.lifecycle());
            assertEquals(ShutdownMode.IMMEDIATE, stopping.shutdownMode());
            assertEquals(List.of(ShutdownMode.GRACEFUL, ShutdownMode.IMMEDIATE), appliedModes);
            assertFalse(supervisor.termination().toCompletableFuture().isDone());
        } finally {
            release.countDown();
            worker.join(TEST_TIMEOUT.toMillis());
        }

        WorkerSnapshot result = terminate(supervisor);
        assertSame(stopping.failure(), result.failure());
        assertEquals(0, result.aliveWorkers());
        assertFalse(worker.isAlive());
    }

    @Test
    void immediateUpgradeBeforeTerminationCommitIsAppliedBeforeTermination() throws Exception {
        CountDownLatch beforeFirstCommit = new CountDownLatch(1);
        CountDownLatch allowFirstCommit = new CountDownLatch(1);
        AtomicInteger commitAttempts = new AtomicInteger();
        List<ShutdownMode> appliedModes = new CopyOnWriteArrayList<>();
        WorkerSupervisor supervisor = WorkerSupervisor.buildForTesting(
                WorkerSupervisor.builder()
                        .name("workers")
                        .expectedWorkers(1)
                        .shutdownTimeout(TEST_TIMEOUT)
                        .stopAction((mode, deadlineNanos) -> appliedModes.add(mode)),
                () -> {
                    if (commitAttempts.getAndIncrement() == 0) {
                        beforeFirstCommit.countDown();
                        await(allowFirstCommit);
                    }
                });
        supervisor.markStarting();
        supervisor.register(new Thread(() -> { }, "unstarted-worker"));
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(beforeFirstCommit.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        allowFirstCommit.countDown();

        WorkerSnapshot result = terminate(supervisor);
        assertEquals(List.of(ShutdownMode.GRACEFUL, ShutdownMode.IMMEDIATE), appliedModes);
        assertEquals(ShutdownMode.IMMEDIATE, result.shutdownMode());
        assertFalse(result.gracefulTermination());
    }

    @Test
    void registeredWorkerStartedAfterTerminationDoesNotRunUserTask() throws Exception {
        AtomicBoolean taskRan = new AtomicBoolean();
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        Thread worker = worker(supervisor, () -> taskRan.set(true), "late-worker");
        supervisor.markStarting();
        supervisor.register(worker);
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        terminate(supervisor);

        worker.start();
        worker.join(TEST_TIMEOUT.toMillis());

        assertFalse(worker.isAlive());
        assertFalse(taskRan.get());
        assertEquals(PipelineLifecycle.TERMINATED, supervisor.snapshot().lifecycle());
    }

    @Test
    void registeredWorkerAdmittedDuringGracefulShutdownCanFinishItsWork() throws Exception {
        CountDownLatch gracefulActionEntered = new CountDownLatch(1);
        CountDownLatch allowGracefulActionToReturn = new CountDownLatch(1);
        CountDownLatch taskRan = new CountDownLatch(1);
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> {
            gracefulActionEntered.countDown();
            await(allowGracefulActionToReturn);
        });
        Thread worker = worker(supervisor, taskRan::countDown, "late-graceful-worker");
        supervisor.markStarting();
        supervisor.register(worker);
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(gracefulActionEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        worker.start();
        worker.join(TEST_TIMEOUT.toMillis());
        allowGracefulActionToReturn.countDown();

        assertFalse(worker.isAlive());
        assertTrue(taskRan.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(terminate(supervisor).gracefulTermination());
    }

    @Test
    void gracefulTerminationCommitWaitsForWorkerAdmittedAfterAllStoppedCheck() throws Exception {
        CountDownLatch beforeCommit = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        WorkerSupervisor supervisor = WorkerSupervisor.buildForTesting(
                WorkerSupervisor.builder()
                        .name("workers")
                        .expectedWorkers(1)
                        .shutdownTimeout(TEST_TIMEOUT)
                        .stopAction((mode, deadlineNanos) -> { }),
                () -> {
                    beforeCommit.countDown();
                    await(allowCommit);
                });
        Thread worker = worker(supervisor, () -> {
            taskEntered.countDown();
            await(releaseTask);
        }, "commit-race-worker");
        supervisor.markStarting();
        supervisor.register(worker);
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(beforeCommit.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        worker.start();
        assertTrue(taskEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        allowCommit.countDown();
        controlThread(supervisor).join(200);
        assertTrue(controlThread(supervisor).isAlive());
        assertFalse(supervisor.termination().toCompletableFuture().isDone());
        releaseTask.countDown();

        WorkerSnapshot result = terminate(supervisor);
        assertTrue(result.gracefulTermination());
        assertEquals(0, result.aliveWorkers());
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
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .registeredWorkers(0).aliveWorkers(1).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .registeredWorkers(2).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .aliveWorkers(-1).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED)
                .aliveWorkers(1)
                .shutdownMode(ShutdownMode.IMMEDIATE)
                .build());
    }

    @Test
    void terminationStageCannotCompleteSupervisor() {
        WorkerSupervisor supervisor = supervisor(1, TEST_TIMEOUT, (mode, deadlineNanos) -> { });
        CompletionStage<WorkerSnapshot> stage = supervisor.termination();
        WorkerSnapshot forged = WorkerSnapshot.builder()
                .name("forged")
                .lifecycle(PipelineLifecycle.TERMINATED)
                .expectedWorkers(1)
                .registeredWorkers(0)
                .aliveWorkers(0)
                .shutdownMode(ShutdownMode.GRACEFUL)
                .gracefulTermination(true)
                .build();

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

    private static WorkerSnapshot.WorkerSnapshotBuilder validWorkerSnapshotBuilder() {
        return WorkerSnapshot.builder()
                .name("workers")
                .lifecycle(PipelineLifecycle.RUNNING)
                .expectedWorkers(1)
                .registeredWorkers(1)
                .aliveWorkers(0)
                .gracefulTermination(false);
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

    private static WorkerSnapshot awaitFailure(WorkerSupervisor supervisor) {
        long deadline = System.nanoTime() + TEST_TIMEOUT.toNanos();
        WorkerSnapshot snapshot = supervisor.snapshot();
        while (snapshot.failure() == null && System.nanoTime() < deadline) {
            Thread.onSpinWait();
            snapshot = supervisor.snapshot();
        }
        assertTrue(snapshot.failure() != null, "等待 supervisor 失败首因超时");
        return snapshot;
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
