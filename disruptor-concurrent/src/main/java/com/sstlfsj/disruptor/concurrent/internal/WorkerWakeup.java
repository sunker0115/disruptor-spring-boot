package com.sstlfsj.disruptor.concurrent.internal;

import lombok.RequiredArgsConstructor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * worker 的睡眠声明与生产者完成发布之间的握手。
 * producer: publication → gate.leave(CAS) → parked.get；
 * worker: parked.set(true) → gate acquire → queue/mailbox/timer 双检。
 * 若 producer 读到 false，该读取在 worker 本轮 volatile true 写之前，故先前 leave
 * 也在 worker 的 gate acquire 之前；acquire 获得该 CAS 或后继 CAS 的 publication。
 * 否则 producer 读到 true 并发出 unpark permit。热提交路径复用原有 leave CAS。
 */
@RequiredArgsConstructor
final class WorkerWakeup {

    private final TaskAdmissionGate gate;
    private final Runnable unpark;
    private final AtomicBoolean parked = new AtomicBoolean();

    void prepareToPark() {
        parked.set(true);
        gate.acquireCompletedPublications();
    }

    void awake() {
        parked.lazySet(false);
    }

    void finishAdmission(long sequence, boolean rollbackOutstanding) {
        gate.leave(sequence, rollbackOutstanding);
        if (sequence >= 0 && parked.get()) {
            unpark.run();
        }
    }
}
