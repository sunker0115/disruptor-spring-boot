package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.CancellationReason;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

/** accepted 且尚未物理清理任务的有序所有权 registry。 */
final class AcceptedTaskRegistry {

    private final ConcurrentSkipListMap<Long, Entry> tasks = new ConcurrentSkipListMap<>();

    void register(AcceptedTask<?> task, AdmissionToken token) {
        Objects.requireNonNull(task, "task 不能为空");
        Objects.requireNonNull(token, "token 不能为空");
        if (!token.active()) {
            throw new IllegalStateException("只能用活动准入令牌登记任务");
        }
        Entry previous = tasks.putIfAbsent(task.acceptedSequence(), new Entry(task, token));
        if (previous != null) {
            throw new IllegalStateException("acceptedSequence 重复=" + task.acceptedSequence());
        }
    }

    boolean rollback(AcceptedTask<?> task) {
        Objects.requireNonNull(task, "task 不能为空");
        Entry entry = tasks.get(task.acceptedSequence());
        return entry != null && entry.task == task
                && tasks.remove(task.acceptedSequence(), entry);
    }

    boolean cancelWaiting(AcceptedTask<?> task, CancellationReason reason) {
        Objects.requireNonNull(task, "task 不能为空");
        Objects.requireNonNull(reason, "reason 不能为空");
        if (!task.markCancelledWaiting()) {
            return false;
        }
        task.future().cancel(reason);
        return true;
    }

    List<Runnable> sweepShutdownNow() {
        List<Runnable> returned = new ArrayList<>();
        for (Entry entry : tasks.values()) {
            AcceptedTask<?> task = entry.task;
            while (true) {
                AcceptedTask.PhysicalState state = task.state();
                if (state == AcceptedTask.PhysicalState.WAITING) {
                    if (!task.tryReturn()) {
                        continue;
                    }
                    task.future().cancel(CancellationReason.SHUTDOWN_NOW);
                    returned.add(task.originalRunnable());
                } else if (state == AcceptedTask.PhysicalState.RUNNING) {
                    task.future().cancel(CancellationReason.SHUTDOWN_NOW);
                }
                break;
            }
        }
        return List.copyOf(returned);
    }

    void cancelAll(CancellationReason reason) {
        Objects.requireNonNull(reason, "reason 不能为空");
        for (Entry entry : tasks.values()) {
            AcceptedTask<?> task = entry.task;
            while (true) {
                AcceptedTask.PhysicalState state = task.state();
                if (state == AcceptedTask.PhysicalState.WAITING) {
                    if (!task.markCancelledWaiting()) {
                        continue;
                    }
                    task.future().cancel(reason);
                } else if (state == AcceptedTask.PhysicalState.RUNNING) {
                    task.future().cancel(reason);
                }
                break;
            }
        }
    }

    boolean terminate(AcceptedTask<?> task) {
        Objects.requireNonNull(task, "task 不能为空");
        if (!task.terminate()) {
            return false;
        }
        Entry entry = tasks.get(task.acceptedSequence());
        if (entry != null && entry.task == task && tasks.remove(task.acceptedSequence(), entry)) {
            entry.token.releaseOutstanding();
        }
        return true;
    }

    int size() {
        return tasks.size();
    }

    private record Entry(AcceptedTask<?> task, AdmissionToken token) {
    }
}
