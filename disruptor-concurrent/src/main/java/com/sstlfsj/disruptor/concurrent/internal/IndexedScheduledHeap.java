package com.sstlfsj.disruptor.concurrent.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 支持 O(log n) 任意任务删除的单线程 timer 最小堆。 */
final class IndexedScheduledHeap {

    private final List<ScheduledTask<?>> heap = new ArrayList<>();

    int size() {
        return heap.size();
    }

    boolean isEmpty() {
        return heap.isEmpty();
    }

    ScheduledTask<?> peek() {
        return heap.isEmpty() ? null : heap.getFirst();
    }

    void add(ScheduledTask<?> task) {
        Objects.requireNonNull(task, "task 不能为空");
        if (task.heapIndex() >= 0) {
            throw new IllegalArgumentException("任务已在 timer heap 中");
        }
        int index = heap.size();
        heap.add(task);
        task.heapIndex(index);
        siftUp(index);
    }

    ScheduledTask<?> poll() {
        return heap.isEmpty() ? null : removeAt(0);
    }

    boolean remove(ScheduledTask<?> task) {
        Objects.requireNonNull(task, "task 不能为空");
        int index = task.heapIndex();
        if (index < 0 || index >= heap.size() || heap.get(index) != task) {
            return false;
        }
        removeAt(index);
        return true;
    }

    private ScheduledTask<?> removeAt(int index) {
        int lastIndex = heap.size() - 1;
        ScheduledTask<?> removed = heap.get(index);
        ScheduledTask<?> moved = heap.remove(lastIndex);
        removed.heapIndex(-1);
        if (index == lastIndex) {
            return removed;
        }
        heap.set(index, moved);
        moved.heapIndex(index);
        if (!siftDown(index)) {
            siftUp(index);
        }
        return removed;
    }

    private void siftUp(int startIndex) {
        int index = startIndex;
        ScheduledTask<?> task = heap.get(index);
        while (index > 0) {
            int parentIndex = (index - 1) >>> 1;
            ScheduledTask<?> parent = heap.get(parentIndex);
            if (task.compareTo(parent) >= 0) {
                break;
            }
            heap.set(index, parent);
            parent.heapIndex(index);
            index = parentIndex;
        }
        heap.set(index, task);
        task.heapIndex(index);
    }

    private boolean siftDown(int startIndex) {
        int index = startIndex;
        int half = heap.size() >>> 1;
        ScheduledTask<?> task = heap.get(index);
        while (index < half) {
            int childIndex = (index << 1) + 1;
            ScheduledTask<?> child = heap.get(childIndex);
            int rightIndex = childIndex + 1;
            if (rightIndex < heap.size() && heap.get(rightIndex).compareTo(child) < 0) {
                childIndex = rightIndex;
                child = heap.get(rightIndex);
            }
            if (task.compareTo(child) <= 0) {
                break;
            }
            heap.set(index, child);
            child.heapIndex(index);
            index = childIndex;
        }
        heap.set(index, task);
        task.heapIndex(index);
        return index > startIndex;
    }
}
