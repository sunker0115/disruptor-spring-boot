package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    void dynamicallySealsRegistrationAndWaitsForEveryWorkerToEnter() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch secondThreadStarted = new CountDownLatch(1);
        CountDownLatch allowSecondAdmission = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.drained = true;
        backend.onGracefulStop = release::countDown;
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread first = worker(supervisor, () -> await(release), "first-worker");
        Runnable secondSupervised = supervisor.supervise(() -> await(release));
        Thread second = Thread.ofPlatform().name("second-worker").unstarted(() -> {
            secondThreadStarted.countDown();
            await(allowSecondAdmission);
            secondSupervised.run();
        });

        supervisor.register(first);
        supervisor.markStarting();
        supervisor.register(second);
        first.start();
        awaitCondition(() -> supervisor.snapshot().startedWorkers() == 1);
        second.start();
        assertTrue(secondThreadStarted.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.sealWorkers();

        assertFalse(supervisor.workersStarted().toCompletableFuture().isDone());
        assertThrows(IllegalStateException.class, supervisor::markRunning);
        allowSecondAdmission.countDown();
        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();

        WorkerSnapshot running = supervisor.snapshot();
        assertTrue(running.registrationSealed());
        assertEquals(2, running.expectedWorkers());
        assertEquals(2, running.registeredWorkers());
        assertEquals(2, running.startedWorkers());
        assertEquals(2, running.aliveWorkers());
        assertTrue(running.reachedRunning());

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(terminate(supervisor).gracefulTermination());
    }

    @Test
    void successfulStartupCallbackCannotBlockTheLastWorkerAdmission() throws Exception {
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch abortCallback = new CountDownLatch(1);
        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        RecordingBackend backend = new RecordingBackend();
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        supervisor.workersStarted().whenComplete((ignored, failure) -> {
            callbackThread.set(Thread.currentThread());
            callbackEntered.countDown();
            awaitTerminationOrAbort(supervisor, abortCallback);
        });
        Thread worker = worker(supervisor, () -> {
            taskEntered.countDown();
            awaitIgnoringInterrupts(releaseWorker);
        }, "callback-isolation-worker");
        supervisor.markStarting();
        supervisor.register(worker);
        supervisor.sealWorkers();

        worker.start();
        assertTrue(callbackEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        boolean workerWasAdmitted = taskEntered.await(200, TimeUnit.MILLISECONDS);
        if (!workerWasAdmitted) {
            abortCallback.countDown();
        }
        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        terminate(supervisor);

        assertTrue(workerWasAdmitted, "启动完成回调不应阻塞最后一个 worker");
        assertTrue(callbackThread.get().isVirtual());
        assertEquals("worker-supervisor-workers-startup-notifier", callbackThread.get().getName());
    }

    @Test
    void failedStartupCallbackCannotBlockShutdownControl() throws Exception {
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch abortCallback = new CountDownLatch(1);
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        RecordingBackend backend = new RecordingBackend();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        supervisor.workersStarted().whenComplete((ignored, failure) -> {
            callbackThread.set(Thread.currentThread());
            callbackEntered.countDown();
            awaitTerminationOrAbort(supervisor, abortCallback);
        });
        supervisor.markStarting();
        supervisor.register(worker(supervisor, () -> { }, "never-started"));
        supervisor.sealWorkers();
        Thread requester = Thread.ofVirtual().name("shutdown-requester")
                .unstarted(() -> supervisor.requestShutdown(ShutdownMode.IMMEDIATE));

        requester.start();
        assertTrue(callbackEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        boolean controlAdvanced = backend.immediateStopped.await(200, TimeUnit.MILLISECONDS);
        if (!controlAdvanced) {
            abortCallback.countDown();
        }
        requester.join(TEST_TIMEOUT.toMillis());
        terminate(supervisor);

        assertTrue(controlAdvanced, "启动失败回调不应阻塞 backend stop");
        assertFalse(requester.isAlive());
        assertTrue(callbackThread.get().isVirtual());
        assertEquals("worker-supervisor-workers-startup-notifier", callbackThread.get().getName());
    }

    @Test
    void lastAdmissionBeforeShutdownCommitsStartupSuccess() throws Exception {
        CountDownLatch beforeAdmission = new CountDownLatch(1);
        CountDownLatch allowAdmission = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Runnable supervised = supervisor.supervise(() -> awaitIgnoringInterrupts(releaseWorker));
        Thread worker = Thread.ofPlatform().name("success-before-shutdown-worker").unstarted(() -> {
            beforeAdmission.countDown();
            await(allowAdmission);
            supervised.run();
        });
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(beforeAdmission.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.sealWorkers();

        allowAdmission.countDown();
        awaitCondition(() -> supervisor.snapshot().startedWorkers() == 1);
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);

        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(SupervisedLifecycle.TERMINATED, terminate(supervisor).lifecycle());
    }

    @Test
    void failureBeforeLastAdmissionCommitsStartupFailure() throws Exception {
        CountDownLatch beforeAdmission = new CountDownLatch(1);
        CountDownLatch allowAdmission = new CountDownLatch(1);
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        Runnable supervised = supervisor.supervise(() -> { });
        Thread worker = Thread.ofPlatform().name("failure-before-success-worker").unstarted(() -> {
            beforeAdmission.countDown();
            awaitIgnoringInterrupts(allowAdmission);
            supervised.run();
        });
        worker.setUncaughtExceptionHandler((thread, failure) -> { });
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(beforeAdmission.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.sealWorkers();

        IllegalStateException original = new IllegalStateException("startup failed");
        supervisor.fail(original);
        allowAdmission.countDown();

        ExecutionException startupFailure = assertThrows(ExecutionException.class,
                () -> supervisor.workersStarted().toCompletableFuture()
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertSame(original, startupFailure.getCause());
        WorkerSnapshot terminated = terminate(supervisor);
        assertEquals(SupervisedLifecycle.TERMINATED, terminated.lifecycle());
        assertSame(original, terminated.failure());
    }

    @Test
    void rejectsNewAdmissionWithoutRunningUserTask() throws Exception {
        RecordingBackend backend = new RecordingBackend();
        AtomicBoolean taskRan = new AtomicBoolean();
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread worker = worker(supervisor, () -> taskRan.set(true), "early-worker");
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));
        supervisor.register(worker);

        worker.start();
        worker.join(TEST_TIMEOUT.toMillis());

        assertFalse(taskRan.get());
        assertInstanceOf(IllegalStateException.class, uncaught.get());
        assertSame(uncaught.get(), terminate(supervisor).failure());
        assertThrows(IllegalStateException.class, () -> supervisor.register(new Thread(() -> { })));
    }

    @Test
    void markRunningRequiresASealedNonEmptyFullyAliveWorkerSet() {
        WorkerSupervisor empty = supervisor(TEST_TIMEOUT, new RecordingBackend());
        empty.markStarting();
        empty.sealWorkers();
        assertThrows(IllegalStateException.class, empty::markRunning);
        empty.requestShutdown(ShutdownMode.IMMEDIATE);
        terminate(empty);

        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        supervisor.markStarting();
        supervisor.register(worker(supervisor, () -> { }, "registered-worker"));
        assertThrows(IllegalStateException.class, supervisor::markRunning);
        supervisor.sealWorkers();
        assertThrows(IllegalStateException.class, supervisor::markRunning);
        assertThrows(IllegalStateException.class, () -> supervisor.register(new Thread(() -> { })));
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        terminate(supervisor);
    }

    @Test
    void gracefulShutdownUsesOneSerializedControlThreadAndObservablePhases() throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.drained = true;
        backend.onGracefulStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> await(releaseWorker));
        backend.supervisor = supervisor;
        Thread caller = Thread.currentThread();

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertEquals(SupervisedLifecycle.QUIESCING, supervisor.snapshot().lifecycle());
        WorkerSnapshot terminated = terminate(supervisor);

        assertEquals(List.of("beginQuiesce", "isDrained", "stop:GRACEFUL"), backend.actions);
        assertEquals(1, backend.maxConcurrentCalls.get());
        assertEquals(1, backend.threads.stream().distinct().count());
        Thread control = backend.threads.getFirst();
        assertTrue(control.isVirtual());
        assertEquals("worker-supervisor-workers", control.getName());
        assertFalse(control == caller);
        assertFalse(backend.calledWithStateLockHeld.get());
        assertEquals(SupervisedLifecycle.STOPPING, backend.lifecycleAtGracefulStop.get());
        assertTrue(terminated.drainCommitted());
        assertTrue(terminated.gracefulStopApplied());
        assertTrue(terminated.gracefulTermination());
    }

    @Test
    void immediateUpgradeWinsWhileGracefulDrainIsPending() throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> awaitIgnoringInterrupts(releaseWorker));
        backend.supervisor = supervisor;

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        assertTrue(backend.probed.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(SupervisedLifecycle.QUIESCING, supervisor.snapshot().lifecycle());
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertEquals(SupervisedLifecycle.STOPPING, supervisor.snapshot().lifecycle());

        WorkerSnapshot terminated = terminate(supervisor);
        assertTrue(backend.actions.contains("stop:IMMEDIATE"));
        assertFalse(backend.actions.contains("stop:GRACEFUL"));
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertFalse(terminated.gracefulTermination());
    }

    @Test
    void immediateStopRunsBackendBeforeInterruptingWorkers() throws Exception {
        CountDownLatch workerEntered = new CountDownLatch(1);
        AtomicReference<Thread> workerReference = new AtomicReference<>();
        AtomicBoolean interruptedAtStop = new AtomicBoolean();
        RecordingBackend backend = new RecordingBackend();
        backend.onImmediateStop = () -> interruptedAtStop.set(
                workerReference.get().isInterrupted());
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread worker = worker(supervisor, () -> {
            workerEntered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
            }
        }, "stop-before-interrupt-worker");
        workerReference.set(worker);
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        supervisor.sealWorkers();
        supervisor.workersStarted().toCompletableFuture()
                .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();
        assertTrue(workerEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        WorkerSnapshot terminated = terminate(supervisor);

        assertFalse(interruptedAtStop.get(), "backend stop 返回前不得先中断 worker");
        assertNull(terminated.failure());
    }

    @Test
    void immediateStopFailureStillInterruptsWorkers() throws Exception {
        RuntimeException original = new RuntimeException("immediate stop failed");
        CountDownLatch workerEntered = new CountDownLatch(1);
        AtomicBoolean workerInterrupted = new AtomicBoolean();
        RecordingBackend backend = new RecordingBackend();
        backend.immediateFailure = original;
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread worker = worker(supervisor, () -> {
            workerEntered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                workerInterrupted.set(true);
            }
        }, "failed-stop-interrupt-worker");
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        supervisor.sealWorkers();
        supervisor.workersStarted().toCompletableFuture()
                .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();
        assertTrue(workerEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        WorkerSnapshot terminated = terminate(supervisor);

        assertTrue(workerInterrupted.get(), "stop(IMMEDIATE) 抛错后仍必须中断 worker");
        assertSame(original, terminated.failure());
    }

    @Test
    void repeatedGracefulRequestDoesNotSkipDrainOrRepeatBackendPhases() throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> awaitIgnoringInterrupts(releaseWorker));

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        awaitCondition(() -> actionCount(backend, "isDrained") >= 2);
        supervisor.requestShutdown(ShutdownMode.GRACEFUL, ShutdownDeadline.after(Duration.ofSeconds(10)));

        assertEquals(SupervisedLifecycle.QUIESCING, supervisor.snapshot().lifecycle());
        assertEquals(1, actionCount(backend, "beginQuiesce"));
        assertFalse(backend.actions.contains("stop:GRACEFUL"));

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        terminate(supervisor);
        assertEquals(1, actionCount(backend, "stop:IMMEDIATE"));
    }

    @Test
    void rechecksStateAfterDrainProbeBeforeCommittingGracefulStop() throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.drained = true;
        backend.onDrainResult = () -> backend.supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> awaitIgnoringInterrupts(releaseWorker));
        backend.supervisor = supervisor;

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        WorkerSnapshot terminated = terminate(supervisor);

        assertFalse(terminated.drainCommitted());
        assertFalse(backend.actions.contains("stop:GRACEFUL"));
        assertTrue(backend.actions.contains("stop:IMMEDIATE"));
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
    }

    @Test
    void backendFailureIsTheFirstCauseAndEscalatesToImmediate() throws Exception {
        RuntimeException original = new RuntimeException("quiesce failed");
        RuntimeException later = new RuntimeException("immediate failed");
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.beginFailure = original;
        backend.immediateFailure = later;
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> awaitIgnoringInterrupts(releaseWorker));
        backend.supervisor = supervisor;

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        WorkerSnapshot terminated = terminate(supervisor);

        assertSame(original, terminated.failure());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertEquals(List.of("beginQuiesce", "stop:IMMEDIATE"), backend.actions);
        assertEquals(1, backend.maxConcurrentCalls.get());
    }

    @Test
    void drainProbeFailureEscalatesToImmediate() throws Exception {
        RuntimeException original = new RuntimeException("drain failed");
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.drainFailure = original;
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> awaitIgnoringInterrupts(releaseWorker));

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        WorkerSnapshot terminated = terminate(supervisor);

        assertSame(original, terminated.failure());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertEquals(List.of("beginQuiesce", "isDrained", "stop:IMMEDIATE"), backend.actions);
    }

    @Test
    void gracefulStopFailureEscalatesToImmediate() throws Exception {
        RuntimeException original = new RuntimeException("graceful stop failed");
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.drained = true;
        backend.gracefulFailure = original;
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> awaitIgnoringInterrupts(releaseWorker));

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        WorkerSnapshot terminated = terminate(supervisor);

        assertSame(original, terminated.failure());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertTrue(terminated.drainCommitted());
        assertFalse(terminated.gracefulStopApplied());
        assertEquals(List.of(
                "beginQuiesce", "isDrained", "stop:GRACEFUL", "stop:IMMEDIATE"), backend.actions);
    }

    @Test
    void workerFailureAfterGracefulStopStillFailsThePipelineAndReachesUncaughtHandler() throws Exception {
        RuntimeException original = new RuntimeException("worker failed while stopping");
        CountDownLatch releaseWorker = new CountDownLatch(1);
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        RecordingBackend backend = new RecordingBackend();
        backend.drained = true;
        backend.onGracefulStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread worker = worker(supervisor, () -> {
            await(releaseWorker);
            throw original;
        }, "failing-stopping-worker");
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        supervisor.sealWorkers();
        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        WorkerSnapshot terminated = terminate(supervisor);

        assertSame(original, terminated.failure());
        assertSame(original, uncaught.get());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertFalse(terminated.gracefulTermination());
        assertEquals(List.of(
                "beginQuiesce", "isDrained", "stop:GRACEFUL", "stop:IMMEDIATE"), backend.actions);
    }

    @Test
    void firstDeadlineIsFrozenAndTimeoutNeverPretendsAWorkerTerminated() throws Exception {
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.drained = true;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> {
            workerEntered.countDown();
            while (releaseWorker.getCount() != 0) {
                try {
                    releaseWorker.await();
                } catch (InterruptedException ignored) {
                    // 测试一个拒绝中断的 worker。
                }
            }
        });
        backend.supervisor = supervisor;
        assertTrue(workerEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        ShutdownDeadline first = ShutdownDeadline.after(Duration.ofMillis(50));
        ShutdownDeadline later = ShutdownDeadline.after(Duration.ofSeconds(10));

        supervisor.requestShutdown(ShutdownMode.GRACEFUL, first);
        supervisor.requestShutdown(ShutdownMode.GRACEFUL, later);
        assertTrue(backend.immediateStopped.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        WorkerSnapshot stopping = supervisor.snapshot();
        assertEquals(SupervisedLifecycle.STOPPING, stopping.lifecycle());
        assertEquals(ShutdownMode.IMMEDIATE, stopping.shutdownMode());
        assertInstanceOf(WorkerSupervisor.WorkerTerminationTimeoutException.class, stopping.failure());
        assertFalse(supervisor.termination().toCompletableFuture().isDone());
        assertEquals(1, actionCount(backend, "stop:GRACEFUL"));
        assertEquals(1, actionCount(backend, "stop:IMMEDIATE"));
        releaseWorker.countDown();
        assertEquals(SupervisedLifecycle.TERMINATED, terminate(supervisor).lifecycle());
    }

    @Test
    void expiredDeadlineAfterPendingDrainDoesNotWaitIndefinitely() throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.onImmediateStop = releaseWorker::countDown;
        WorkerSupervisor supervisor = runningSupervisor(backend, () -> awaitIgnoringInterrupts(releaseWorker));
        ShutdownDeadline deadline = ShutdownDeadline.after(Duration.ofMillis(5));
        backend.onDrainResult = () -> {
            while (!deadline.isExpired()) {
                Thread.onSpinWait();
            }
        };

        supervisor.requestShutdown(ShutdownMode.GRACEFUL, deadline);

        assertTrue(backend.immediateStopped.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        WorkerSnapshot terminated = terminate(supervisor);
        assertInstanceOf(WorkerSupervisor.WorkerTerminationTimeoutException.class, terminated.failure());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
    }

    @Test
    void normalExitInStartingRunningAndQuiescingTriggersFailStop() throws Exception {
        assertUnexpectedExit(SupervisedLifecycle.STARTING);
        assertUnexpectedExit(SupervisedLifecycle.RUNNING);
        assertUnexpectedExit(SupervisedLifecycle.QUIESCING);
    }

    @Test
    void workerFailureKeepsOriginalThrowableAndReachesUncaughtHandler() throws Exception {
        RuntimeException original = new RuntimeException("worker failed");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(release);
            throw original;
        }, "failing-worker");
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.sealWorkers();
        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();

        release.countDown();
        WorkerSnapshot terminated = terminate(supervisor);
        worker.join(TEST_TIMEOUT.toMillis());

        assertSame(original, terminated.failure());
        assertSame(original, uncaught.get());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
    }

    @Test
    void workerCanRequestShutdownWithoutWaitingForItself() throws Exception {
        RecordingBackend backend = new RecordingBackend();
        AtomicReference<WorkerSupervisor> reference = new AtomicReference<>();
        CountDownLatch allowRequest = new CountDownLatch(1);
        CountDownLatch returnedFromRequest = new CountDownLatch(1);
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        reference.set(supervisor);
        Thread worker = worker(supervisor, () -> {
            await(allowRequest);
            reference.get().requestShutdown(ShutdownMode.IMMEDIATE);
            returnedFromRequest.countDown();
        }, "self-stopping-worker");
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        supervisor.sealWorkers();
        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();
        allowRequest.countDown();

        assertTrue(returnedFromRequest.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(SupervisedLifecycle.TERMINATED, terminate(supervisor).lifecycle());
    }

    @Test
    void shutdownBeforeAllWorkersStartCompletesStartupSignalExceptionally() {
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        supervisor.markStarting();
        supervisor.register(worker(supervisor, () -> { }, "never-started"));
        supervisor.sealWorkers();

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);

        assertThrows(Exception.class,
                () -> supervisor.workersStarted().toCompletableFuture()
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        terminate(supervisor);
    }

    @Test
    void shutdownWaitsForInFlightStartToFinishAndRegistrationToBeSealed() throws Exception {
        CountDownLatch startEntered = new CountDownLatch(1);
        CountDownLatch allowStart = new CountDownLatch(1);
        AtomicBoolean taskRan = new AtomicBoolean();
        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        Thread worker = new PausingStartThread(
                supervisor.supervise(() -> taskRan.set(true)), startEntered, allowStart);
        worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));
        Thread starter = Thread.ofVirtual().name("worker-starter").unstarted(() -> {
            try {
                worker.start();
            } catch (Throwable failure) {
                startFailure.set(failure);
            }
        });
        supervisor.markStarting();
        supervisor.register(worker);
        starter.start();
        assertTrue(startEntered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertFalse(supervisor.termination().toCompletableFuture().isDone());
        allowStart.countDown();
        starter.join(TEST_TIMEOUT.toMillis());
        assertNull(startFailure.get());
        supervisor.sealWorkers();

        WorkerSnapshot terminated = terminate(supervisor);
        assertFalse(taskRan.get());
        assertFalse(worker.isAlive());
        assertInstanceOf(IllegalStateException.class, uncaught.get());
        assertTrue(terminated.registrationSealed());
        assertEquals(1, terminated.startedWorkers());
        assertEquals(0, terminated.aliveWorkers());
    }

    @Test
    void startupMayFinishRegistrationAndSealAfterConcurrentShutdown() {
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        supervisor.markStarting();
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertFalse(supervisor.termination().toCompletableFuture().isDone());

        supervisor.register(worker(supervisor, () -> { }, "never-started"));
        supervisor.sealWorkers();

        WorkerSnapshot terminated = terminate(supervisor);
        assertTrue(terminated.registrationSealed());
        assertEquals(1, terminated.registeredWorkers());
        assertEquals(0, terminated.startedWorkers());
    }

    @Test
    void sealingRequiresAnActiveStartupProtocol() {
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());

        assertThrows(IllegalStateException.class, supervisor::sealWorkers);

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertTrue(terminate(supervisor).registrationSealed());
    }

    @Test
    void gracefulStopBeforeRunningDoesNotClaimGracefulTermination() {
        RecordingBackend backend = new RecordingBackend();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);

        supervisor.requestShutdown(ShutdownMode.GRACEFUL);
        WorkerSnapshot terminated = terminate(supervisor);

        assertTrue(backend.actions.contains("stop:GRACEFUL"));
        assertTrue(terminated.gracefulStopApplied());
        assertFalse(terminated.reachedRunning());
        assertFalse(terminated.drainCommitted());
        assertFalse(terminated.gracefulTermination());
    }

    @Test
    void exposesReadOnlyCompletionStages() {
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        CompletionStage<Void> workersStarted = supervisor.workersStarted();
        CompletionStage<WorkerSnapshot> termination = supervisor.termination();

        assertTrue(workersStarted.toCompletableFuture().complete(null));
        assertFalse(supervisor.workersStarted().toCompletableFuture().isDone());
        assertTrue(termination.toCompletableFuture().complete(validTerminatedSnapshot()));
        assertFalse(supervisor.termination().toCompletableFuture().isDone());

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertEquals("workers", terminate(supervisor).name());
    }

    @Test
    void validatesWorkerSnapshotStateFacts() {
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .startedWorkers(0).aliveWorkers(1).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .registeredWorkers(0).startedWorkers(1).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .registrationSealed(true).expectedWorkers(2).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .lifecycle(SupervisedLifecycle.RUNNING).aliveWorkers(0).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .lifecycle(SupervisedLifecycle.TERMINATED).aliveWorkers(1).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .lifecycle(SupervisedLifecycle.TERMINATED)
                .registrationSealed(false)
                .expectedWorkers(0)
                .aliveWorkers(0)
                .shutdownMode(ShutdownMode.IMMEDIATE)
                .build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .drainCommitted(true).build());

        WorkerSnapshot nonGraceful = validWorkerSnapshotBuilder()
                .lifecycle(SupervisedLifecycle.TERMINATED)
                .aliveWorkers(0)
                .shutdownMode(ShutdownMode.IMMEDIATE)
                .reachedRunning(true)
                .drainCommitted(true)
                .gracefulStopApplied(true)
                .build();
        assertFalse(nonGraceful.gracefulTermination());
        assertTrue(validTerminatedSnapshot().gracefulTermination());
    }

    @Test
    void validatesDeadlineAndBuilderInputs() {
        assertThrows(NullPointerException.class, () -> ShutdownDeadline.after(null));
        assertThrows(IllegalArgumentException.class,
                () -> ShutdownDeadline.after(Duration.ofNanos(-1)));
        assertTrue(ShutdownDeadline.after(Duration.ZERO).isExpired());
        ShutdownDeadline saturated = ShutdownDeadline.after(Duration.ofSeconds(Long.MAX_VALUE));
        assertTrue(saturated.remainingNanos() > 0);
        assertFalse(saturated.isExpired());

        assertThrows(NullPointerException.class, () -> WorkerSupervisor.builder()
                .shutdownTimeout(TEST_TIMEOUT).shutdownBackend(new RecordingBackend()).build());
        assertThrows(IllegalArgumentException.class, () -> WorkerSupervisor.builder()
                .name(" ").shutdownTimeout(TEST_TIMEOUT).shutdownBackend(new RecordingBackend()).build());
        assertThrows(IllegalArgumentException.class, () -> WorkerSupervisor.builder()
                .name("workers").shutdownTimeout(Duration.ZERO)
                .shutdownBackend(new RecordingBackend()).build());
        assertThrows(NullPointerException.class, () -> WorkerSupervisor.builder()
                .name("workers").shutdownTimeout(TEST_TIMEOUT).build());
    }

    @Test
    void exposesGenericLifecycleAndTrulyUnboundedDeadline() {
        ShutdownDeadline unbounded = ShutdownDeadline.unbounded();
        ShutdownDeadline bounded = ShutdownDeadline.after(TEST_TIMEOUT);

        assertSame(unbounded, ShutdownDeadline.unbounded());
        assertFalse(unbounded.isBounded());
        assertFalse(unbounded.isExpired());
        assertEquals(Long.MAX_VALUE, unbounded.remainingNanos());
        assertEquals(Long.MAX_VALUE, unbounded.deadlineNanos());
        assertTrue(bounded.isBounded());

        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        assertEquals(SupervisedLifecycle.NEW, supervisor.snapshot().lifecycle());
    }

    private static void assertUnexpectedExit(SupervisedLifecycle phase) throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread worker = worker(supervisor, () -> {
            entered.countDown();
            await(release);
        }, "exiting-" + phase.name().toLowerCase());
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        assertTrue(entered.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        supervisor.sealWorkers();
        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (phase != SupervisedLifecycle.STARTING) {
            supervisor.markRunning();
        }
        if (phase == SupervisedLifecycle.QUIESCING) {
            supervisor.requestShutdown(ShutdownMode.GRACEFUL);
            awaitCondition(() -> supervisor.snapshot().lifecycle() == SupervisedLifecycle.QUIESCING);
        }

        release.countDown();
        WorkerSnapshot terminated = terminate(supervisor);

        assertInstanceOf(WorkerSupervisor.UnexpectedWorkerExitException.class, terminated.failure());
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
    }

    private static WorkerSupervisor runningSupervisor(RecordingBackend backend, Runnable task)
            throws Exception {
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread worker = worker(supervisor, task, "managed-worker");
        supervisor.markStarting();
        supervisor.register(worker);
        worker.start();
        supervisor.sealWorkers();
        supervisor.workersStarted().toCompletableFuture().get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        supervisor.markRunning();
        return supervisor;
    }

    private static WorkerSupervisor supervisor(Duration timeout, ShutdownBackend backend) {
        WorkerSupervisor supervisor = WorkerSupervisor.builder()
                .name("workers")
                .shutdownTimeout(timeout)
                .shutdownBackend(backend)
                .build();
        if (backend instanceof RecordingBackend recordingBackend) {
            recordingBackend.supervisor = supervisor;
        }
        return supervisor;
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

    private static WorkerSnapshot.WorkerSnapshotBuilder validWorkerSnapshotBuilder() {
        return WorkerSnapshot.builder()
                .name("workers")
                .lifecycle(SupervisedLifecycle.RUNNING)
                .registrationSealed(true)
                .expectedWorkers(1)
                .registeredWorkers(1)
                .startedWorkers(1)
                .aliveWorkers(1)
                .reachedRunning(true);
    }

    private static WorkerSnapshot validTerminatedSnapshot() {
        return validWorkerSnapshotBuilder()
                .lifecycle(SupervisedLifecycle.TERMINATED)
                .aliveWorkers(0)
                .shutdownMode(ShutdownMode.GRACEFUL)
                .drainCommitted(true)
                .gracefulStopApplied(true)
                .build();
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

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        while (latch.getCount() != 0) {
            try {
                latch.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitTerminationOrAbort(
            WorkerSupervisor supervisor,
            CountDownLatch abortCallback) {
        while (!supervisor.termination().toCompletableFuture().isDone()
                && abortCallback.getCount() != 0) {
            try {
                supervisor.termination().toCompletableFuture().get(20, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                // 回调必须能暴露同步完成导致的 worker/request 阻塞，测试清理通过 abortCallback 退出。
            } catch (TimeoutException ignored) {
                // 继续等待终止或测试清理信号。
            } catch (ExecutionException impossible) {
                throw new AssertionError("termination 不应异常完成", impossible);
            }
        }
    }

    private static int actionCount(RecordingBackend backend, String action) {
        return (int) backend.actions.stream().filter(action::equals).count();
    }

    private static final class PausingStartThread extends Thread {

        private final CountDownLatch startEntered;
        private final CountDownLatch allowStart;

        private PausingStartThread(
                Runnable target,
                CountDownLatch startEntered,
                CountDownLatch allowStart) {
            super(target, "pausing-start-worker");
            this.startEntered = startEntered;
            this.allowStart = allowStart;
        }

        @Override
        public synchronized void start() {
            startEntered.countDown();
            await(allowStart);
            super.start();
        }
    }

    private static void awaitCondition(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TEST_TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(condition.getAsBoolean(), "等待条件超时");
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }

    private static final class RecordingBackend implements ShutdownBackend {

        private final List<String> actions = new CopyOnWriteArrayList<>();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrentCalls = new AtomicInteger();
        private final AtomicInteger maxConcurrentCalls = new AtomicInteger();
        private final AtomicBoolean calledWithStateLockHeld = new AtomicBoolean();
        private final AtomicReference<SupervisedLifecycle> lifecycleAtGracefulStop = new AtomicReference<>();
        private final CountDownLatch probed = new CountDownLatch(1);
        private final CountDownLatch immediateStopped = new CountDownLatch(1);
        private volatile WorkerSupervisor supervisor;
        private volatile boolean drained;
        private volatile Runnable onDrainResult = () -> { };
        private volatile Runnable onGracefulStop = () -> { };
        private volatile Runnable onImmediateStop = () -> { };
        private volatile Throwable beginFailure;
        private volatile Throwable drainFailure;
        private volatile Throwable gracefulFailure;
        private volatile Throwable immediateFailure;

        @Override
        public void beginQuiesce() throws Throwable {
            enter("beginQuiesce");
            try {
                throwIfPresent(beginFailure);
            } finally {
                exit();
            }
        }

        @Override
        public boolean isDrained() throws Throwable {
            enter("isDrained");
            try {
                probed.countDown();
                onDrainResult.run();
                throwIfPresent(drainFailure);
                return drained;
            } finally {
                exit();
            }
        }

        @Override
        public void stop(ShutdownMode mode) throws Throwable {
            enter("stop:" + mode);
            try {
                if (mode == ShutdownMode.GRACEFUL) {
                    lifecycleAtGracefulStop.set(supervisor.snapshot().lifecycle());
                    onGracefulStop.run();
                    throwIfPresent(gracefulFailure);
                } else {
                    onImmediateStop.run();
                    immediateStopped.countDown();
                    throwIfPresent(immediateFailure);
                }
            } finally {
                exit();
            }
        }

        private void enter(String action) {
            actions.add(action);
            threads.add(Thread.currentThread());
            int depth = concurrentCalls.incrementAndGet();
            maxConcurrentCalls.accumulateAndGet(depth, Math::max);
            if (supervisor != null) {
                calledWithStateLockHeld.compareAndSet(false, holdsStateLock(supervisor));
            }
        }

        private void exit() {
            concurrentCalls.decrementAndGet();
        }

        private static boolean holdsStateLock(WorkerSupervisor supervisor) {
            try {
                var field = WorkerSupervisor.class.getDeclaredField("stateLock");
                field.setAccessible(true);
                return Thread.holdsLock(field.get(supervisor));
            } catch (ReflectiveOperationException failure) {
                throw new AssertionError("无法读取 supervisor 状态锁", failure);
            }
        }

        private static void throwIfPresent(Throwable failure) throws Throwable {
            if (failure != null) {
                throw failure;
            }
        }
    }
}
