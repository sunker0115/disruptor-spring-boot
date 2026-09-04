package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/** 固定 Group 的唯一启动、owner gate、关闭会话和真实终止协调器。 */
public final class GroupLifecycleCoordinator {

    private static final Logger log = LoggerFactory.getLogger(GroupLifecycleCoordinator.class);
    private static final long WAIT_NANOS = 1_000_000L;

    private final String name;
    private final Duration shutdownTimeout;
    private final OwnerAdmissionGate admissionGate = new OwnerAdmissionGate();
    private final Object stateLock = new Object();
    private final Object broadcastLock = new Object();
    private final CompletableFuture<Void> startup = new CompletableFuture<>();
    private final CompletionStage<Void> startupView = startup.minimalCompletionStage();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final CompletionStage<Void> terminationView = termination.minimalCompletionStage();
    private final AtomicBoolean terminationCoordinatorStarted = new AtomicBoolean();
    private final AtomicBoolean startupOutcomeClaimed = new AtomicBoolean();

    private List<ChildControl> children = List.of();
    private boolean childrenBound;
    private SupervisedLifecycle lifecycle = SupervisedLifecycle.NEW;
    private Throwable failure;
    private Throwable startupFailure;
    private ShutdownMode shutdownMode;
    private ShutdownDeadline shutdownDeadline;
    private ShutdownMode broadcastedMode;

    public GroupLifecycleCoordinator(String name, Duration shutdownTimeout) {
        this.name = requireName(name);
        this.shutdownTimeout = requirePositive(shutdownTimeout);
    }

    public void bindChildren(List<ChildControl> children) {
        List<ChildControl> copy = List.copyOf(
                Objects.requireNonNull(children, "children 不能为空"));
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("children 不能为空");
        }
        synchronized (stateLock) {
            if (childrenBound || lifecycle != SupervisedLifecycle.NEW) {
                throw new IllegalStateException("Group children 只能在 NEW 状态绑定一次");
            }
            this.children = copy;
            childrenBound = true;
        }
        copy.forEach(child -> child.loop().termination()
                .whenComplete((snapshot, stageFailure) ->
                        onChildTermination(child, stageFailure)));
    }

    public AdmissionLease tryAcquireAdmission() {
        return admissionGate.tryAcquire();
    }

    public void childFailed(EventLoop child, Throwable cause) {
        Objects.requireNonNull(child, "child 不能为空");
        Objects.requireNonNull(cause, "cause 不能为空");
        synchronized (stateLock) {
            requireChildrenBound();
            if (children.stream().noneMatch(control -> control.loop() == child)) {
                throw new IllegalArgumentException("EventLoop 不属于当前 Group：" + child.name());
            }
        }
        failChild(cause);
    }

    public CompletionStage<Void> start() {
        synchronized (stateLock) {
            requireChildrenBound();
            if (lifecycle == SupervisedLifecycle.STARTING
                    || lifecycle == SupervisedLifecycle.RUNNING) {
                return startupView;
            }
            if (lifecycle != SupervisedLifecycle.NEW) {
                if (startupFailure == null) {
                    startupFailure = new GroupStartupAbortedException(name);
                }
                publishStartupFailureIfTerminated();
                return startupView;
            }
            lifecycle = SupervisedLifecycle.STARTING;
        }
        Thread.ofVirtual().name(name + "-group-start").start(this::startChildren);
        return startupView;
    }

    public CompletionStage<Void> termination() {
        return terminationView;
    }

    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        Objects.requireNonNull(mode, "mode 不能为空");
        Objects.requireNonNull(deadline, "deadline 不能为空");
        ShutdownRequest request = beginShutdown(mode, deadline);
        if (request == null) {
            return;
        }
        awaitAdmissionsUninterruptibly();
        broadcast(request.mode(), request.deadline());
        startTerminationCoordinator();
    }

    public List<Runnable> shutdownNow(ShutdownDeadline deadline) {
        Objects.requireNonNull(deadline, "deadline 不能为空");
        ShutdownRequest request = beginShutdown(ShutdownMode.IMMEDIATE, deadline);
        if (request == null) {
            return List.of();
        }
        awaitAdmissionsUninterruptibly();
        List<Runnable> returned = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        synchronized (broadcastLock) {
            for (ChildControl child : children) {
                try {
                    returned.addAll(child.shutdownNow().shutdownNow(request.deadline()));
                } catch (Throwable failure) {
                    failures.add(failure);
                    log.warn("立即停止 Group child 失败：group={}，child={}",
                            name, child.loop().name(), failure);
                }
            }
            broadcastedMode = ShutdownMode.IMMEDIATE;
        }
        recordFailures(failures);
        startTerminationCoordinator();
        return List.copyOf(returned);
    }

    public StateSnapshot snapshot() {
        synchronized (stateLock) {
            return new StateSnapshot(
                    lifecycle,
                    admissionGate.accepting(),
                    failure,
                    shutdownMode,
                    shutdownDeadline);
        }
    }

    private void startChildren() {
        Throwable startFailure = null;
        List<CompletableFuture<Void>> stages = new ArrayList<>(children.size());
        for (ChildControl child : children) {
            try {
                stages.add(child.starter().start().toCompletableFuture());
            } catch (Throwable failure) {
                if (startFailure == null) {
                    startFailure = failure;
                } else if (startFailure != failure) {
                    startFailure.addSuppressed(failure);
                }
            }
        }
        try {
            CompletableFuture.allOf(stages.toArray(CompletableFuture[]::new)).join();
        } catch (Throwable failure) {
            Throwable unwrapped = unwrap(failure);
            if (startFailure == null) {
                startFailure = unwrapped;
            } else if (startFailure != unwrapped) {
                startFailure.addSuppressed(unwrapped);
            }
        }
        if (startFailure != null) {
            failStartup(startFailure);
            return;
        }

        boolean started = false;
        synchronized (stateLock) {
            if (lifecycle == SupervisedLifecycle.STARTING) {
                lifecycle = SupervisedLifecycle.RUNNING;
                admissionGate.open();
                started = true;
            } else if (startupFailure == null) {
                startupFailure = new GroupStartupAbortedException(name);
            }
        }
        if (started) {
            publishStartupSuccess();
        } else {
            failStartup(startupFailure);
        }
    }

    private void failStartup(Throwable cause) {
        ShutdownRequest request;
        synchronized (stateLock) {
            admissionGate.close();
            if (startupFailure == null) {
                startupFailure = cause;
            }
            if (failure == null) {
                failure = startupFailure;
            }
            if (shutdownDeadline == null) {
                shutdownDeadline = ShutdownDeadline.after(shutdownTimeout);
            }
            shutdownMode = ShutdownMode.IMMEDIATE;
            lifecycle = SupervisedLifecycle.STOPPING;
            request = new ShutdownRequest(shutdownMode, shutdownDeadline);
        }
        awaitAdmissionsUninterruptibly();
        broadcast(request.mode(), request.deadline());
        startTerminationCoordinator();
    }

    private ShutdownRequest beginShutdown(ShutdownMode mode, ShutdownDeadline deadline) {
        synchronized (stateLock) {
            requireChildrenBound();
            if (lifecycle == SupervisedLifecycle.TERMINATED) {
                return null;
            }
            SupervisedLifecycle previous = lifecycle;
            admissionGate.close();
            if (shutdownDeadline == null) {
                shutdownDeadline = deadline;
            }
            if (shutdownMode == null || mode == ShutdownMode.IMMEDIATE) {
                shutdownMode = mode;
            }
            if ((previous == SupervisedLifecycle.NEW
                    || previous == SupervisedLifecycle.STARTING)
                    && startupFailure == null) {
                startupFailure = new GroupStartupAbortedException(name);
            }
            lifecycle = previous == SupervisedLifecycle.RUNNING
                    && shutdownMode == ShutdownMode.GRACEFUL
                    ? SupervisedLifecycle.QUIESCING
                    : SupervisedLifecycle.STOPPING;
            return new ShutdownRequest(shutdownMode, shutdownDeadline);
        }
    }

    private void broadcast(ShutdownMode mode, ShutdownDeadline deadline) {
        List<Throwable> failures = new ArrayList<>();
        synchronized (broadcastLock) {
            if (broadcastedMode == ShutdownMode.IMMEDIATE
                    || broadcastedMode == mode) {
                return;
            }
            for (ChildControl child : children) {
                try {
                    child.shutdownRequester().request(mode, deadline);
                } catch (Throwable failure) {
                    failures.add(failure);
                    log.warn("请求停止 Group child 失败：group={}，child={}，mode={}",
                            name, child.loop().name(), mode, failure);
                }
            }
            broadcastedMode = mode;
        }
        recordFailures(failures);
    }

    private void startTerminationCoordinator() {
        if (terminationCoordinatorStarted.compareAndSet(false, true)) {
            Thread.ofVirtual().name(name + "-group-shutdown")
                    .start(this::coordinateTermination);
        }
    }

    private void coordinateTermination() {
        while (!allChildrenTerminated()) {
            maybeEscalateExpiredDeadline();
            LockSupport.parkNanos(this, WAIT_NANOS);
        }
        List<Throwable> childFailures = new ArrayList<>();
        for (ChildControl child : children) {
            Throwable childFailure = child.loop().snapshot().failure();
            if (childFailure != null) {
                childFailures.add(childFailure);
            }
        }
        recordFailures(childFailures);
        synchronized (stateLock) {
            lifecycle = SupervisedLifecycle.TERMINATED;
        }
        Thread.ofVirtual().name(name + "-group-termination-notifier")
                .start(() -> {
                    termination.complete(null);
                    publishStartupFailureIfTerminated();
                });
    }

    private void maybeEscalateExpiredDeadline() {
        ShutdownRequest upgrade = null;
        synchronized (stateLock) {
            if (shutdownMode == ShutdownMode.GRACEFUL
                    && shutdownDeadline != null
                    && shutdownDeadline.isExpired()) {
                shutdownMode = ShutdownMode.IMMEDIATE;
                lifecycle = SupervisedLifecycle.STOPPING;
                if (failure == null) {
                    failure = new GroupShutdownTimeoutException(name);
                }
                upgrade = new ShutdownRequest(shutdownMode, shutdownDeadline);
            }
        }
        if (upgrade != null) {
            broadcast(upgrade.mode(), upgrade.deadline());
        }
    }

    private void onChildTermination(ChildControl child, Throwable stageFailure) {
        Throwable cause = stageFailure;
        if (cause == null) {
            cause = child.loop().snapshot().failure();
        }
        if (cause != null) {
            failChild(cause);
            return;
        }
        synchronized (stateLock) {
            if (lifecycle != SupervisedLifecycle.RUNNING) {
                return;
            }
        }
        failChild(new UnexpectedChildTerminationException(name, child.loop().name()));
    }

    private void failChild(Throwable cause) {
        ShutdownRequest request;
        synchronized (stateLock) {
            boolean startupFailureDetected = lifecycle == SupervisedLifecycle.STARTING;
            boolean runningFailureDetected = lifecycle == SupervisedLifecycle.RUNNING
                    || lifecycle == SupervisedLifecycle.QUIESCING;
            if (!startupFailureDetected && !runningFailureDetected) {
                return;
            }
            admissionGate.close();
            if (startupFailureDetected && startupFailure == null) {
                startupFailure = cause;
            }
            if (failure == null) {
                failure = startupFailureDetected ? startupFailure : cause;
            }
            if (shutdownDeadline == null) {
                shutdownDeadline = ShutdownDeadline.after(shutdownTimeout);
            }
            shutdownMode = ShutdownMode.IMMEDIATE;
            lifecycle = SupervisedLifecycle.STOPPING;
            request = new ShutdownRequest(shutdownMode, shutdownDeadline);
        }
        awaitAdmissionsUninterruptibly();
        broadcast(request.mode(), request.deadline());
        startTerminationCoordinator();
    }

    private boolean allChildrenTerminated() {
        return children.stream().allMatch(child ->
                child.loop().termination().toCompletableFuture().isDone());
    }

    private void recordFailures(List<? extends Throwable> failures) {
        if (failures.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            for (Throwable next : failures) {
                Throwable unwrapped = unwrap(next);
                if (failure == null) {
                    failure = unwrapped;
                } else if (failure != unwrapped
                        && !containsIdentity(failure.getSuppressed(), unwrapped)) {
                    failure.addSuppressed(unwrapped);
                }
            }
        }
    }

    private void awaitAdmissionsUninterruptibly() {
        boolean interrupted = false;
        while (true) {
            try {
                admissionGate.awaitClosedAdmissions();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void publishStartupSuccess() {
        if (startupOutcomeClaimed.compareAndSet(false, true)) {
            Thread.ofVirtual().name(name + "-group-startup-notifier")
                    .start(() -> startup.complete(null));
        }
    }

    private void publishStartupFailureIfTerminated() {
        Throwable cause;
        synchronized (stateLock) {
            if (lifecycle != SupervisedLifecycle.TERMINATED) {
                return;
            }
            cause = startupFailure != null
                    ? startupFailure
                    : new GroupStartupAbortedException(name);
        }
        if (startupOutcomeClaimed.compareAndSet(false, true)) {
            startup.completeExceptionally(cause);
        }
    }

    private void requireChildrenBound() {
        if (!childrenBound) {
            throw new IllegalStateException("Group children 尚未绑定");
        }
    }

    private static boolean containsIdentity(Throwable[] failures, Throwable expected) {
        for (Throwable failure : failures) {
            if (failure == expected) {
                return true;
            }
        }
        return false;
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private static String requireName(String value) {
        Objects.requireNonNull(value, "name 不能为空");
        if (value.isBlank()) {
            throw new IllegalArgumentException("name 不能为空白");
        }
        return value;
    }

    private static Duration requirePositive(Duration value) {
        Objects.requireNonNull(value, "shutdownTimeout 不能为空");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout 必须为正数，实际值=" + value);
        }
        value.toNanos();
        return value;
    }

    public record StateSnapshot(
            SupervisedLifecycle lifecycle,
            boolean accepting,
            Throwable failure,
            ShutdownMode shutdownMode,
            ShutdownDeadline shutdownDeadline) {
    }

    public record ChildControl(
            EventLoop loop,
            ChildStarter starter,
            ChildShutdownRequester shutdownRequester,
            ChildShutdownNow shutdownNow) {
        public ChildControl {
            Objects.requireNonNull(loop, "loop 不能为空");
            Objects.requireNonNull(starter, "starter 不能为空");
            Objects.requireNonNull(shutdownRequester, "shutdownRequester 不能为空");
            Objects.requireNonNull(shutdownNow, "shutdownNow 不能为空");
        }
    }

    @FunctionalInterface
    public interface ChildStarter {
        CompletionStage<Void> start();
    }

    @FunctionalInterface
    public interface ChildShutdownRequester {
        void request(ShutdownMode mode, ShutdownDeadline deadline);
    }

    @FunctionalInterface
    public interface ChildShutdownNow {
        List<Runnable> shutdownNow(ShutdownDeadline deadline);
    }

    public interface AdmissionLease extends AutoCloseable {
        @Override
        void close();
    }

    private record ShutdownRequest(ShutdownMode mode, ShutdownDeadline deadline) {
    }

    private static final class OwnerAdmissionGate {
        private boolean open;
        private long activeAdmissions;

        private synchronized void open() {
            if (open) {
                throw new IllegalStateException("Group admission gate 已打开");
            }
            open = true;
        }

        private synchronized void close() {
            open = false;
            notifyAll();
        }

        private synchronized AdmissionLease tryAcquire() {
            if (!open) {
                return null;
            }
            activeAdmissions++;
            return new Lease(this);
        }

        private synchronized boolean accepting() {
            return open;
        }

        private synchronized void awaitClosedAdmissions() throws InterruptedException {
            while (activeAdmissions != 0) {
                wait();
            }
        }

        private synchronized void release() {
            if (activeAdmissions <= 0) {
                throw new IllegalStateException("没有 Group admission lease 可归还");
            }
            activeAdmissions--;
            notifyAll();
        }
    }

    private static final class Lease implements AdmissionLease {
        private final OwnerAdmissionGate gate;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(OwnerAdmissionGate gate) {
            this.gate = gate;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                gate.release();
            }
        }
    }

    public static final class GroupShutdownTimeoutException extends IllegalStateException {
        private GroupShutdownTimeoutException(String name) {
            super("EventLoopGroup 超过共享关闭截止时间：" + name);
        }
    }

    private static final class GroupStartupAbortedException extends IllegalStateException {
        private GroupStartupAbortedException(String name) {
            super("EventLoopGroup 启动在完成前被关闭：" + name);
        }
    }

    private static final class UnexpectedChildTerminationException extends IllegalStateException {
        private UnexpectedChildTerminationException(String group, String child) {
            super("Group child 在运行期间意外终止：group=" + group + "，child=" + child);
        }
    }
}
