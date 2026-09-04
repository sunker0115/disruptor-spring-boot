package com.sstlfsj.disruptor.concurrent.internal;

/** TaskQueue 槽位内部信封，包含任务或可跨过的 tombstone。 */
final class TaskEnvelope {

    static final TaskEnvelope TOMBSTONE = new TaskEnvelope(null);

    private final AcceptedTask<?> task;

    private TaskEnvelope(AcceptedTask<?> task) {
        this.task = task;
    }

    static TaskEnvelope task(AcceptedTask<?> task) {
        return new TaskEnvelope(java.util.Objects.requireNonNull(task, "task 不能为空"));
    }

    boolean tombstone() {
        return task == null;
    }

    AcceptedTask<?> task() {
        return task;
    }
}
