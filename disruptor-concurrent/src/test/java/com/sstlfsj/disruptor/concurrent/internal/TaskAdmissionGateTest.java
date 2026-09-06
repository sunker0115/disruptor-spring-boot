package com.sstlfsj.disruptor.concurrent.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskAdmissionGateTest {

    @Test
    void boundedCapacityRejectsTheFirstEntryBeyondTheLimit() {
        TaskAdmissionGate gate = TaskAdmissionGate.bounded(1);
        assertFalse(gate.tryEnter());
        gate.open();

        assertTrue(gate.tryEnter());
        assertFalse(gate.tryEnter());
        assertEquals(1, gate.activePublishers());
        assertEquals(1, gate.outstanding());

        gate.leave(false);
        assertEquals(0, gate.activePublishers());
        assertEquals(1, gate.outstanding());
        assertFalse(gate.tryEnter());

        gate.completeBatch(1);
        assertTrue(gate.tryEnter());
        gate.leave(true);
        assertEquals(0, gate.outstanding());
    }

    @Test
    void failedClaimCanRollbackOutstandingWithoutLeakingCapacity() {
        TaskAdmissionGate gate = TaskAdmissionGate.bounded(1);
        gate.open();

        assertTrue(gate.tryEnter());
        gate.leave(true);

        assertEquals(0, gate.activePublishers());
        assertEquals(0, gate.outstanding());
        assertTrue(gate.tryEnter());
        gate.leave(true);
    }

    @Test
    void closeRejectsNewEntriesAndAwaitDrainedWaitsForLastPublisher() throws Exception {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded(ledger);
        gate.open();
        assertTrue(gate.tryEnter());
        assertTrue(ledger.tryCommitClaim(0));
        gate.closeForAdmissions();
        assertFalse(gate.isAccepting());
        assertFalse(gate.tryEnter());

        CountDownLatch waiting = new CountDownLatch(1);
        Thread waiter = Thread.ofPlatform().start(() -> {
            waiting.countDown();
            gate.awaitDrained();
        });
        assertTrue(waiting.await(1, TimeUnit.SECONDS));
        Thread.sleep(10);
        assertTrue(waiter.isAlive());

        gate.leave(false);
        waiter.join(1_000);
        assertFalse(waiter.isAlive());
        gate.completeBatch(1);
        assertEquals(0, gate.outstanding());
    }

    @Test
    void awaitDrainedParksPreInterruptedWaiterAndRestoresInterruptStatus() throws Exception {
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded();
        gate.open();
        assertTrue(gate.tryEnter());
        AtomicBoolean interruptedAfterWait = new AtomicBoolean();
        CountDownLatch entered = new CountDownLatch(1);
        Thread waiter = Thread.ofPlatform().start(() -> {
            Thread.currentThread().interrupt();
            entered.countDown();
            gate.awaitDrained();
            interruptedAfterWait.set(Thread.currentThread().isInterrupted());
        });
        try {
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            awaitState(waiter, Thread.State.WAITING);
            assertFalse(waiter.isInterrupted(), "等待期间必须暂存而不是保留中断标记空转");
        } finally {
            gate.leave(true);
            waiter.join(1_000);
        }

        assertFalse(waiter.isAlive());
        assertTrue(interruptedAfterWait.get());
    }

    @Test
    void unboundedTracksOutstandingWithoutACapacityLimit() {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded(ledger);
        gate.open();

        for (int index = 0; index < 4; index++) {
            assertTrue(gate.tryEnter());
            assertEquals(index, gate.outstanding(), "进入 publisher 不等于成功占号");
            assertTrue(ledger.tryCommitClaim(index));
            gate.leave(false);
        }

        assertEquals(4, gate.outstanding());
        gate.completeBatch(3);
        assertEquals(1, gate.outstanding());
        gate.completeBatch(1);
        assertEquals(0, gate.outstanding());
    }

    @Test
    void boundedCapacityCannotExceedPackedOutstandingField() {
        assertThrows(IllegalArgumentException.class,
                () -> TaskAdmissionGate.bounded(1L << 31));
    }

    @Test
    void unboundedSequenceExhaustionLeavesNoPublisherOrOutstandingLeak() throws Exception {
        UnboundedTaskLedger ledger = UnboundedTaskLedgerTest.exhaustedAfterOneMoreClaim();
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded(ledger);
        gate.open();
        assertTrue(gate.tryEnter());
        assertTrue(ledger.tryCommitClaim(ledger.candidateSequence()));
        gate.leave(false);
        assertEquals(1, gate.outstanding());
        gate.completeBatch(1);
        assertTrue(gate.tryEnter());
        assertThrows(IllegalStateException.class, ledger::candidateSequence);
        gate.leave(true);
        assertEquals(Long.MAX_VALUE - 1, ledger.claimedCursor());
        assertEquals(0, gate.outstanding());
        assertEquals(0, gate.activePublishers());
    }

    @Test
    void failureBeforeClaimCannotRollbackAnotherPublishersOutstanding() {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded(ledger);
        gate.open();
        assertTrue(gate.tryEnter());
        assertTrue(ledger.tryCommitClaim(0));
        gate.leave(false);
        assertTrue(gate.tryEnter());
        assertEquals(1, gate.outstanding());
        gate.leave(true);
        assertEquals(1, gate.outstanding());
        assertEquals(0, gate.activePublishers());
        gate.completeBatch(1);
        assertEquals(0, gate.outstanding());
    }

    private static void awaitState(Thread thread, Thread.State expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (thread.getState() != expected) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("线程未进入 " + expected + "，实际=" + thread.getState());
            }
            Thread.sleep(1);
        }
    }
}
