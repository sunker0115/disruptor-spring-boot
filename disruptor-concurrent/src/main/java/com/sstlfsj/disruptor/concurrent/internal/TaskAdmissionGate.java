package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.core.ShutdownDeadline;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** 统一任务准入、活动提交握手和全生命容量的 gate。 */
final class TaskAdmissionGate {

    private final Object lock = new Object();
    private final long capacity;
    private final boolean bounded;

    private State state = State.NEW;
    private long activeAdmissions;
    private long outstanding;

    private TaskAdmissionGate(long capacity, boolean bounded) {
        this.capacity = capacity;
        this.bounded = bounded;
    }

    static TaskAdmissionGate bounded(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity 必须为正数，实际值=" + capacity);
        }
        return new TaskAdmissionGate(capacity, true);
    }

    static TaskAdmissionGate unbounded() {
        return new TaskAdmissionGate(Long.MAX_VALUE, false);
    }

    void open() {
        synchronized (lock) {
            if (state == State.CLOSED) {
                throw new IllegalStateException("已关闭的准入 gate 不能重新打开");
            }
            state = State.OPEN;
        }
    }

    void closeForAdmissions() {
        synchronized (lock) {
            state = State.CLOSED;
            lock.notifyAll();
        }
    }

    AdmissionToken tryAcquire() {
        synchronized (lock) {
            if (state != State.OPEN || bounded && outstanding >= capacity) {
                return null;
            }
            activeAdmissions++;
            outstanding++;
            return new AdmissionToken(this);
        }
    }

    boolean awaitAdmissions(ShutdownDeadline deadline) throws InterruptedException {
        Objects.requireNonNull(deadline, "deadline 不能为空");
        synchronized (lock) {
            while (activeAdmissions != 0) {
                if (deadline.isExpired()) {
                    return false;
                }
                if (!deadline.isBounded()) {
                    lock.wait();
                    continue;
                }
                long remaining = deadline.remainingNanos();
                if (remaining <= 0) {
                    return false;
                }
                long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
                int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
                lock.wait(millis, nanos);
            }
            return true;
        }
    }

    long activeAdmissions() {
        synchronized (lock) {
            return activeAdmissions;
        }
    }

    long outstanding() {
        synchronized (lock) {
            return outstanding;
        }
    }

    long remainingCapacity() {
        synchronized (lock) {
            return bounded ? capacity - outstanding : Long.MAX_VALUE;
        }
    }

    boolean accepting() {
        synchronized (lock) {
            return state == State.OPEN;
        }
    }

    void finishAdmission(boolean releaseOutstanding) {
        synchronized (lock) {
            if (activeAdmissions <= 0) {
                throw new IllegalStateException("没有可完成的活动准入");
            }
            activeAdmissions--;
            if (releaseOutstanding) {
                outstanding--;
            }
            lock.notifyAll();
        }
    }

    void releaseOutstanding() {
        synchronized (lock) {
            if (outstanding <= 0) {
                throw new IllegalStateException("没有可归还的 outstanding 容量");
            }
            outstanding--;
            lock.notifyAll();
        }
    }

    private enum State {
        NEW,
        OPEN,
        CLOSED
    }
}
