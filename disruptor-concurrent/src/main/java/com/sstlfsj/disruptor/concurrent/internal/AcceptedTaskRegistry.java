package com.sstlfsj.disruptor.concurrent.internal;

import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

/** tracked/scheduled accepted 任务的 admission-ticket 有序索引。 */
final class AcceptedTaskRegistry {

    private final ConcurrentSkipListMap<Long, AcceptedTask<?>> tasks =
            new ConcurrentSkipListMap<>();

    void register(AcceptedTask<?> task) {
        Objects.requireNonNull(task, "task 不能为空");
        AcceptedTask<?> previous = tasks.putIfAbsent(task.acceptedSequence(), task);
        if (previous != null) {
            throw new IllegalStateException(
                    "acceptedSequence 重复=" + task.acceptedSequence());
        }
    }

    void remove(AcceptedTask<?> task) {
        Objects.requireNonNull(task, "task 不能为空");
        tasks.remove(task.acceptedSequence(), task);
    }

    void terminalizeAndRemove(AcceptedTask<?> task) {
        if (task == null) {
            return;
        }
        task.terminate();
        tasks.remove(task.acceptedSequence(), task);
    }

    void scanForShutdown(ShutdownAction action) {
        Objects.requireNonNull(action, "action 不能为空");
        for (AcceptedTask<?> task : tasks.values()) {
            action.handle(task);
        }
    }

    int size() {
        return tasks.size();
    }
}

@FunctionalInterface
interface ShutdownAction {
    void handle(AcceptedTask<?> task);
}
