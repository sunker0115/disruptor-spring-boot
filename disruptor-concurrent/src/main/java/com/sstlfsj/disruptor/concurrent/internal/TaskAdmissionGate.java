package com.sstlfsj.disruptor.concurrent.internal;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/** 统一拥有生命周期、活动发布者和全生命周期容量账本的无锁 gate。 */
final class TaskAdmissionGate {

    private static final long FIELD_MAX = (1L << 31) - 1;
    private static final int OUTSTANDING_SHIFT = 31;
    private static final int LIFECYCLE_SHIFT = 62;
    private static final long PUBLISHERS_MASK = FIELD_MAX;
    private static final long LIFECYCLE_MASK = 3L << LIFECYCLE_SHIFT;
    private static final long OPEN = 1L << LIFECYCLE_SHIFT;
    private static final long CLOSED = 2L << LIFECYCLE_SHIFT;
    private static final int SPIN_LIMIT = 64;

    private final AtomicLong admission = new AtomicLong();
    private final UnboundedTaskLedger unboundedLedger;
    private final ConcurrentLinkedQueue<Thread> drainWaiters = new ConcurrentLinkedQueue<>();
    private final long capacity;
    private final boolean bounded;

    private TaskAdmissionGate(long capacity, boolean bounded, UnboundedTaskLedger unboundedLedger) {
        this.capacity = capacity;
        this.bounded = bounded;
        this.unboundedLedger = unboundedLedger;
    }

    static TaskAdmissionGate bounded(long capacity) {
        if (capacity <= 0 || capacity > FIELD_MAX) {
            throw new IllegalArgumentException(
                    "capacity 必须在 [1, 2^31-1] 范围内，实际值=" + capacity);
        }
        return new TaskAdmissionGate(capacity, true, null);
    }

    static TaskAdmissionGate unbounded() {
        return unbounded(new UnboundedTaskLedger());
    }

    static TaskAdmissionGate unbounded(UnboundedTaskLedger ledger) {
        return new TaskAdmissionGate(Long.MAX_VALUE, false,
                Objects.requireNonNull(ledger, "ledger 不能为空"));
    }

    void open() {
        while (true) {
            long current = admission.get();
            long lifecycle = current & LIFECYCLE_MASK;
            if (lifecycle == CLOSED) {
                throw new IllegalStateException("已关闭的准入 gate 不能重新打开");
            }
            if (lifecycle == OPEN) {
                return;
            }
            if (admission.compareAndSet(current, current | OPEN)) {
                return;
            }
        }
    }

    void closeForAdmissions() {
        while (true) {
            long current = admission.get();
            if ((current & LIFECYCLE_MASK) == CLOSED) {
                return;
            }
            long closed = current & ~LIFECYCLE_MASK | CLOSED;
            if (admission.compareAndSet(current, closed)) {
                if (publishers(closed) == 0) {
                    unparkDrainWaiters();
                }
                return;
            }
        }
    }

    boolean isAccepting() {
        return (admission.get() & LIFECYCLE_MASK) == OPEN;
    }

    boolean tryEnter() {
        while (true) {
            long current = admission.get();
            if ((current & LIFECYCLE_MASK) != OPEN) {
                return false;
            }
            long activePublishers = publishers(current);
            if (activePublishers == FIELD_MAX) {
                return false;
            }
            long next = current + 1;
            if (bounded) {
                long outstanding = boundedOutstanding(current);
                if (outstanding >= capacity) {
                    return false;
                }
                next += 1L << OUTSTANDING_SHIFT;
            }
            if (admission.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    void leave(boolean rollbackOutstanding) {
        leave(-1, rollbackOutstanding);
    }

    /** sequence < 0 表示只进入 publisher、尚未占号；无界账本此时没有回滚额度。 */
    void leave(long sequence, boolean rollbackOutstanding) {
        if (!bounded) {
            if (rollbackOutstanding && sequence >= 0) {
                unboundedLedger.rollbackClaim();
            }
            leavePublisherOnly();
            return;
        }
        while (true) {
            long current = admission.get();
            long activePublishers = publishers(current);
            long outstanding = boundedOutstanding(current);
            if (activePublishers == 0) {
                throw new IllegalStateException("没有可离开的活动 publisher");
            }
            if (rollbackOutstanding && outstanding == 0) {
                throw new IllegalStateException("没有可回滚的 outstanding");
            }
            long next = current - 1;
            if (rollbackOutstanding) {
                next -= 1L << OUTSTANDING_SHIFT;
            }
            if (admission.compareAndSet(current, next)) {
                if (activePublishers == 1) {
                    unparkDrainWaiters();
                }
                return;
            }
        }
    }

    void completeBatch(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count 必须为正数，实际值=" + count);
        }
        if (!bounded) {
            unboundedLedger.completeBatch(count);
            return;
        }
        while (true) {
            long current = admission.get();
            long outstanding = boundedOutstanding(current);
            if (outstanding < count) {
                throw new IllegalStateException("完成数量超过 outstanding：count=" + count
                        + "，outstanding=" + outstanding);
            }
            long next = current - ((long) count << OUTSTANDING_SHIFT);
            if (admission.compareAndSet(current, next)) {
                return;
            }
        }
    }

    long outstanding() {
        return bounded ? boundedOutstanding(admission.get()) : unboundedLedger.outstanding();
    }

    long activePublishers() {
        return publishers(admission.get());
    }

    /** 与 leave 的 CAS 同步；即使 word 的计数值复原，volatile 读取仍获得最近的写入。 */
    void acquireCompletedPublications() {
        admission.get();
    }

    void awaitDrained() {
        boolean interrupted = Thread.interrupted();
        try {
            for (int spin = 0; spin < SPIN_LIMIT; spin++) {
                if (activePublishers() == 0) {
                    return;
                }
                Thread.onSpinWait();
            }
            Thread current = Thread.currentThread();
            drainWaiters.add(current);
            try {
                while (activePublishers() != 0) {
                    interrupted |= Thread.interrupted();
                    LockSupport.park(this);
                }
            } finally {
                drainWaiters.remove(current);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void leavePublisherOnly() {
        while (true) {
            long current = admission.get();
            long activePublishers = publishers(current);
            if (activePublishers == 0) {
                throw new IllegalStateException("没有可离开的活动 publisher");
            }
            if (admission.compareAndSet(current, current - 1)) {
                if (activePublishers == 1) {
                    unparkDrainWaiters();
                }
                return;
            }
        }
    }

    private void unparkDrainWaiters() {
        for (Thread waiter : drainWaiters) {
            LockSupport.unpark(waiter);
        }
    }

    private static long publishers(long word) {
        return word & PUBLISHERS_MASK;
    }

    private static long boundedOutstanding(long word) {
        return word >>> OUTSTANDING_SHIFT & FIELD_MAX;
    }
}
