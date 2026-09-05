package com.sstlfsj.disruptor.concurrent.internal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerWakeupTest {

    @ParameterizedTest(name = "bounded = {0}")
    @ValueSource(booleans = {true, false})
    void publicationReleaseMustPrecedeTheSignalWhileProducerIsPaused(
            boolean bounded) throws Exception {
        TaskAdmissionGate gate = bounded
                ? TaskAdmissionGate.bounded(8) : TaskAdmissionGate.unbounded();
        gate.open();
        assertTrue(gate.tryEnter());
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

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(2, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
