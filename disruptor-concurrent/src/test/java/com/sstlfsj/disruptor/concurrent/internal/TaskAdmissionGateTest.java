package com.sstlfsj.disruptor.concurrent.internal;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded();
        gate.open();
        assertTrue(gate.tryEnter());
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
    void unboundedTracksOutstandingWithoutACapacityLimit() {
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded();
        gate.open();

        for (int index = 0; index < 4; index++) {
            assertTrue(gate.tryEnter());
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
    void unboundedOutstandingRefusesOverflowWithoutWrapping() throws Exception {
        TaskAdmissionGate gate = TaskAdmissionGate.unbounded();
        gate.open();
        Field field = TaskAdmissionGate.class.getDeclaredField("unboundedOutstanding");
        field.setAccessible(true);
        AtomicLong outstanding = (AtomicLong) field.get(gate);
        outstanding.set(Long.MAX_VALUE);

        assertFalse(gate.tryEnter());
        assertEquals(Long.MAX_VALUE, gate.outstanding());
        assertEquals(0, gate.activePublishers());
    }
}
