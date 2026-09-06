package com.sstlfsj.disruptor.concurrent.internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

/** 占号即准入计数；回滚由失败的 producer 写，物理完成仅由 worker 批量写。 */
final class UnboundedTaskLedger {

    private static final VarHandle COMPLETED;

    static {
        try {
            COMPLETED = MethodHandles.lookup().findVarHandle(
                    UnboundedTaskLedger.class, "completed", long.class);
        } catch (NoSuchFieldException | IllegalAccessException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private final AtomicLong claimed = new AtomicLong();
    private final AtomicLong rolledBack = new AtomicLong();
    private long completed;

    long candidateSequence() {
        long candidate = claimed.get();
        if (candidate == Long.MAX_VALUE) {
            throw new IllegalStateException("TaskQueue sequence 已耗尽");
        }
        return candidate;
    }

    boolean tryCommitClaim(long expected) {
        if (expected < 0) {
            throw new IllegalArgumentException("candidate sequence 不能为负数");
        }
        if (expected == Long.MAX_VALUE) {
            throw new IllegalStateException("TaskQueue sequence 已耗尽");
        }
        return claimed.compareAndSet(expected, expected + 1);
    }

    /** 仅能由已成功占号、随后发布 tombstone 的 producer 调用一次。 */
    void rollbackClaim() {
        while (true) {
            long done = (long) COMPLETED.getAcquire(this);
            long rollback = rolledBack.get();
            if (claimed.get() - done <= rollback) {
                throw new IllegalStateException("没有可回滚的 claim");
            }
            if (rolledBack.compareAndSet(rollback, rollback + 1)) {
                return;
            }
        }
    }

    /** 单写者：必须在槽位、timer 和 registry 的物理清理之后调用。 */
    void completeBatch(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count 必须为正数，实际值=" + count);
        }
        long outstanding = outstanding();
        if (count > outstanding) {
            throw new IllegalStateException("完成数量超过 outstanding：count=" + count
                    + "，outstanding=" + outstanding);
        }
        COMPLETED.setRelease(this, completed + count);
    }

    long claimedCursor() {
        return claimed.get() - 1;
    }

    long outstanding() {
        while (true) {
            // completion/rollback 均发生在 claim 之后；先 acquire 扣减项再读 claimed。
            long done = (long) COMPLETED.getAcquire(this);
            long rollback = rolledBack.get();
            long outstanding = claimed.get() - rollback - done;
            if (outstanding >= 0) {
                return outstanding;
            }
        }
    }
}
