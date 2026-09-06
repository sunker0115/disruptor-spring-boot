package com.sstlfsj.disruptor.concurrent.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerWakeupTest {

    @ParameterizedTest(name = "bounded = {0}")
    @ValueSource(booleans = {true, false})
    void publicationReleaseMustPrecedeTheSignalWhileProducerIsPaused(
            boolean bounded) throws Exception {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        TaskAdmissionGate gate = bounded
                ? TaskAdmissionGate.bounded(8) : TaskAdmissionGate.unbounded(ledger);
        gate.open();
        assertTrue(gate.tryEnter());
        if (!bounded) {
            assertTrue(ledger.tryCommitClaim(0));
        }
        CountDownLatch signalEntered = new CountDownLatch(1);
        CountDownLatch resumeSignal = new CountDownLatch(1);
        WorkerWakeup wakeup = new WorkerWakeup(gate, () -> {
            signalEntered.countDown();
            await(resumeSignal);
        });
        wakeup.prepareToPark();
        try (ExecutorService producer = Executors.newSingleThreadExecutor()) {
            Future<?> submitted = producer.submit(() -> wakeup.finishAdmission(0, false));
            try {
                assertTrue(signalEntered.await(2, TimeUnit.SECONDS));
                // signal 可能尚未送达；此时 worker 的二次检查必须能 acquire 已发布数据。
                assertEquals(0, gate.activePublishers(),
                        "parked 检查与信号之前必须已完成 admission release");
            } finally {
                resumeSignal.countDown();
            }
            submitted.get(2, TimeUnit.SECONDS);
        }
        assertEquals(1, gate.outstanding());
        gate.completeBatch(1);
    }

    @Test
    void allocationFailureLeavesOnlyPublisherAndLaterTombstoneRollsBackItsClaim() {
        AtomicBoolean failExpansion = new AtomicBoolean(true);
        UnboundedTaskQueue queue = new UnboundedTaskQueue(1, 0, (id, size) -> {
            if (id == 1 && failExpansion.getAndSet(false)) {
                throw new OutOfMemoryError("注入扩段失败");
            }
            return new UnboundedTaskQueue.Segment(id, size);
        });
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded(queue.ledger());
        AtomicInteger signals = new AtomicInteger();
        WorkerWakeup wakeup = new WorkerWakeup(gate, signals::incrementAndGet);
        gate.open();
        wakeup.prepareToPark();

        assertTrue(gate.tryEnter());
        long accepted = queue.tryClaim();
        queue.writeOrdinary(accepted, () -> { });
        queue.publish(accepted);
        wakeup.finishAdmission(accepted, false);
        assertEquals(1, gate.outstanding());
        assertEquals(1, signals.get());

        assertTrue(gate.tryEnter());
        assertThrows(OutOfMemoryError.class, queue::tryClaim);
        wakeup.finishAdmission(-1, true);
        assertEquals(0, gate.activePublishers());
        assertEquals(1, gate.outstanding(), "占号前失败不能扣除另一项已接受任务");
        assertEquals(1, signals.get(), "没有 publication 时不发送唤醒");

        assertTrue(gate.tryEnter());
        long failed = queue.tryClaim();
        assertEquals(1, failed);
        queue.writeTombstone(failed);
        queue.publish(failed);
        wakeup.finishAdmission(failed, true);
        assertEquals(1, gate.outstanding());
        assertEquals(0, gate.activePublishers());
        assertEquals(2, signals.get(), "tombstone 仍须唤醒消费者");

        assertTrue(queue.poll());
        assertTrue(queue.tryStartCurrentOrdinary());
        queue.advanceConsumer();
        queue.terminalizeCurrentOrdinary();
        queue.releaseCurrentSlot();
        gate.completeBatch(1);
        assertTrue(queue.poll());
        assertEquals(TaskType.TOMBSTONE, queue.currentType());
        queue.advanceConsumer();
        queue.releaseCurrentSlot();
        assertFalse(queue.poll());
        assertEquals(0, gate.outstanding());
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(2, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
