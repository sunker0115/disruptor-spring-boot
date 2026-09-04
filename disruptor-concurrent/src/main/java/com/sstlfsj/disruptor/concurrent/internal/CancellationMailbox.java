package com.sstlfsj.disruptor.concurrent.internal;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/** 生产者向单消费者 worker 传递物理取消清理的邮箱。 */
final class CancellationMailbox {

    private final ConcurrentLinkedQueue<AcceptedTask<?>> queue = new ConcurrentLinkedQueue<>();

    boolean offer(AcceptedTask<?> task) {
        Objects.requireNonNull(task, "task 不能为空");
        if (!task.markCancellationQueued()) {
            return false;
        }
        queue.add(task);
        return true;
    }

    AcceptedTask<?> poll() {
        AcceptedTask<?> task = queue.poll();
        if (task != null) {
            task.clearCancellationQueued();
        }
        return task;
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }
}
