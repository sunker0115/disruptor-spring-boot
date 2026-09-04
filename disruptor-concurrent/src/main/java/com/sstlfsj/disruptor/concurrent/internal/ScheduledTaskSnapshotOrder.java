package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.ScheduledTaskSnapshot;

/** 统一 timer heap 与 ScheduledFuture 的顺序规则。 */
final class ScheduledTaskSnapshotOrder {

    private ScheduledTaskSnapshotOrder() {
    }

    static int compare(ScheduledTaskSnapshot left, ScheduledTaskSnapshot right, long nowNanos) {
        if (left.triggerNanos() != right.triggerNanos()) {
            long leftDelay = left.triggerNanos() - nowNanos;
            long rightDelay = right.triggerNanos() - nowNanos;
            if (leftDelay <= 0 && rightDelay > 0) {
                return -1;
            }
            if (leftDelay > 0 && rightDelay <= 0) {
                return 1;
            }
            long triggerDelta = left.triggerNanos() - right.triggerNanos();
            if (triggerDelta != 0) {
                return triggerDelta < 0 ? -1 : 1;
            }
        }
        int priorityOrder = Integer.compare(right.priority(), left.priority());
        return priorityOrder != 0
                ? priorityOrder
                : Long.compare(left.acceptedSequence(), right.acceptedSequence());
    }
}
