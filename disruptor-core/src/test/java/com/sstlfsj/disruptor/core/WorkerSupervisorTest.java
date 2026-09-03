package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerSupervisorTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    @Test
    void dynamicallySealsRegistrationAndWaitsForEveryWorkerToEnter() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        RecordingBackend backend = new RecordingBackend();
        backend.drained = true;
        backend.onGracefulStop = release::countDown;
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, backend);
        Thread first = worker(supervisor, () -> await(release), "first-worker");
        Thread second = worker(supervisor, () -> await(release), "second-worker");

        supervisor.register(first);
        supervisor.markStarting();
        supervisor.register(second);
        first.start();
        awaitCondition(() -> supervisor.snapshot().startedWorkers() == 1);
        supervisor.sealWorkers();

        assertFalse(supervisor.workersStarted().toCompletableFuture().isDone());
        assertThrows(IllegalStateException.class, supervisor::markRunning);
        second.start();
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
        assertEquals(PipelineLifecycle.QUIESCING, supervisor.snapshot().lifecycle());
        WorkerSnapshot terminated = terminate(supervisor);

        assertEquals(List.of("beginQuiesce", "isDrained", "stop:GRACEFUL"), backend.actions);
        assertEquals(1, backend.maxConcurrentCalls.get());
        assertEquals(1, backend.threads.stream().distinct().count());
        Thread control = backend.threads.getFirst();
        assertTrue(control.isVirtual());
        assertEquals("worker-supervisor-workers", control.getName());
        assertFalse(control == caller);
        assertFalse(backend.calledWithStateLockHeld.get());
        assertEquals(PipelineLifecycle.STOPPING, backend.lifecycleAtGracefulStop.get());
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
        assertEquals(PipelineLifecycle.QUIESCING, supervisor.snapshot().lifecycle());
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertEquals(PipelineLifecycle.STOPPING, supervisor.snapshot().lifecycle());

        WorkerSnapshot terminated = terminate(supervisor);
        assertTrue(backend.actions.contains("stop:IMMEDIATE"));
        assertFalse(backend.actions.contains("stop:GRACEFUL"));
        assertEquals(ShutdownMode.IMMEDIATE, terminated.shutdownMode());
        assertFalse(terminated.gracefulTermination());
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

        assertEquals(PipelineLifecycle.QUIESCING, supervisor.snapshot().lifecycle());
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
        assertEquals(PipelineLifecycle.STOPPING, stopping.lifecycle());
        assertEquals(ShutdownMode.IMMEDIATE, stopping.shutdownMode());
        assertInstanceOf(WorkerSupervisor.WorkerTerminationTimeoutException.class, stopping.failure());
        assertFalse(supervisor.termination().toCompletableFuture().isDone());
        assertEquals(1, actionCount(backend, "stop:GRACEFUL"));
        assertEquals(1, actionCount(backend, "stop:IMMEDIATE"));
        releaseWorker.countDown();
        assertEquals(PipelineLifecycle.TERMINATED, terminate(supervisor).lifecycle());
    }

    @Test
    void normalExitInStartingRunningAndQuiescingTriggersFailStop() throws Exception {
        assertUnexpectedExit(PipelineLifecycle.STARTING);
        assertUnexpectedExit(PipelineLifecycle.RUNNING);
        assertUnexpectedExit(PipelineLifecycle.QUIESCING);
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
        assertEquals(PipelineLifecycle.TERMINATED, terminate(supervisor).lifecycle());
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
    void terminationWaitsForAStartedThreadThatHasNotEnteredItsWrapper() throws Exception {
        CountDownLatch threadStarted = new CountDownLatch(1);
        CountDownLatch allowAdmission = new CountDownLatch(1);
        CountDownLatch interruptedBeforeAdmission = new CountDownLatch(1);
        AtomicBoolean taskRan = new AtomicBoolean();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        Runnable supervised = supervisor.supervise(() -> taskRan.set(true));
        Thread worker = Thread.ofPlatform().name("pre-admission-worker").unstarted(() -> {
            threadStarted.countDown();
            while (allowAdmission.getCount() != 0) {
                try {
                    allowAdmission.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    interruptedBeforeAdmission.countDown();
                }
            }
            supervised.run();
        });
        worker.setUncaughtExceptionHandler((thread, failure) -> { });
        supervisor.markStarting();
        supervisor.register(worker);
        supervisor.sealWorkers();
        worker.start();
        assertTrue(threadStarted.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        assertFalse(supervisor.termination().toCompletableFuture().isDone());
        assertTrue(interruptedBeforeAdmission.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        allowAdmission.countDown();

        WorkerSnapshot terminated = terminate(supervisor);
        assertFalse(taskRan.get());
        assertFalse(worker.isAlive());
        assertEquals(1, terminated.startedWorkers());
        assertEquals(0, terminated.aliveWorkers());
    }

    @Test
    void workerStartingAfterTerminationIsRejectedWithoutChangingTerminalSnapshot() throws Exception {
        AtomicBoolean taskRan = new AtomicBoolean();
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
        Thread lateWorker = worker(supervisor, () -> taskRan.set(true), "late-worker");
        lateWorker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));
        supervisor.markStarting();
        supervisor.register(lateWorker);
        supervisor.sealWorkers();
        supervisor.requestShutdown(ShutdownMode.IMMEDIATE);
        WorkerSnapshot terminated = terminate(supervisor);

        lateWorker.start();
        lateWorker.join(TEST_TIMEOUT.toMillis());

        assertFalse(taskRan.get());
        assertInstanceOf(IllegalStateException.class, uncaught.get());
        assertEquals(terminated, supervisor.snapshot());
        assertEquals(0, terminated.startedWorkers());
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
                .lifecycle(PipelineLifecycle.RUNNING).aliveWorkers(0).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED).aliveWorkers(1).build());
        assertThrows(IllegalArgumentException.class, () -> validWorkerSnapshotBuilder()
                .drainCommitted(true).build());

        WorkerSnapshot nonGraceful = validWorkerSnapshotBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED)
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
        assertTrue(ShutdownDeadline.after(Duration.ofSeconds(Long.MAX_VALUE)).remainingNanos() > 0);

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

    private static void assertUnexpectedExit(PipelineLifecycle phase) throws Exception {
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
        if (phase != PipelineLifecycle.STARTING) {
            supervisor.markRunning();
        }
        if (phase == PipelineLifecycle.QUIESCING) {
            supervisor.requestShutdown(ShutdownMode.GRACEFUL);
            awaitCondition(() -> supervisor.snapshot().lifecycle() == PipelineLifecycle.QUIESCING);
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
                .lifecycle(PipelineLifecycle.RUNNING)
                .registrationSealed(true)
                .expectedWorkers(1)
                .registeredWorkers(1)
                .startedWorkers(1)
                .aliveWorkers(1)
                .reachedRunning(true);
    }

    private static WorkerSnapshot validTerminatedSnapshot() {
        return validWorkerSnapshotBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED)
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

    private static int actionCount(RecordingBackend backend, String action) {
        return (int) backend.actions.stream().filter(action::equals).count();
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
        private final AtomicReference<PipelineLifecycle> lifecycleAtGracefulStop = new AtomicReference<>();
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
