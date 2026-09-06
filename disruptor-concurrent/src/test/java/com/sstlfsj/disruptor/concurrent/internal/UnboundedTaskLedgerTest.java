package com.sstlfsj.disruptor.concurrent.internal;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnboundedTaskLedgerTest {

    @Test
    void concurrentClaimsAreUniqueAndContiguous() throws Exception {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        Set<Long> sequences = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService producers = Executors.newFixedThreadPool(4)) {
            List<Future<?>> results = new ArrayList<>();
            for (int producer = 0; producer < 4; producer++) {
                results.add(producers.submit(() -> {
                    assertTrue(start.await(2, TimeUnit.SECONDS));
                    long previous = -1;
                    for (int index = 0; index < 1_000; index++) {
                        long sequence = claim(ledger);
                        assertTrue(sequence > previous);
                        assertTrue(sequences.add(sequence));
                        previous = sequence;
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> result : results) {
                result.get(5, TimeUnit.SECONDS);
            }
        }
        assertEquals(4_000, sequences.size());
        for (long sequence = 0; sequence < 4_000; sequence++) {
            assertTrue(sequences.contains(sequence));
        }
        assertEquals(3_999, ledger.claimedCursor());
        assertEquals(4_000, ledger.outstanding());
    }

    @Test
    void candidateAndFailedCasDoNotCreateRollbackCredit() {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        assertEquals(0, ledger.candidateSequence());
        assertEquals(-1, ledger.claimedCursor());
        assertThrows(IllegalStateException.class, ledger::rollbackClaim);
        assertTrue(ledger.tryCommitClaim(0));
        assertFalse(ledger.tryCommitClaim(0));
        ledger.rollbackClaim();
        assertEquals(0, ledger.outstanding());
        assertEquals(0, ledger.claimedCursor(), "回滚不复用已占用的 sequence");
        assertThrows(IllegalStateException.class, ledger::rollbackClaim);
        assertEquals(1, claim(ledger));
        assertEquals(1, ledger.outstanding());
    }

    @Test
    void singleWriterCompletesBatchesAfterRollbackAndDrainsToZero() {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        for (int index = 0; index < 6; index++) {
            assertEquals(index, claim(ledger));
        }
        ledger.rollbackClaim();
        ledger.completeBatch(3);
        assertEquals(2, ledger.outstanding());
        ledger.completeBatch(2);
        assertEquals(0, ledger.outstanding());
        assertThrows(IllegalStateException.class, () -> ledger.completeBatch(1));
        assertThrows(IllegalArgumentException.class, () -> ledger.completeBatch(0));
        assertThrows(IllegalArgumentException.class, () -> ledger.completeBatch(-1));
        assertEquals(0, ledger.outstanding());
    }

    @Test
    void snapshotsStayNonNegativeDuringClaimsRollbacksAndSingleWriterCompletion()
            throws Exception {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        ConcurrentLinkedQueue<Long> accepted = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService racers = Executors.newFixedThreadPool(3);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int producer = 0; producer < 2; producer++) {
                results.add(racers.submit(() -> {
                    assertTrue(start.await(2, TimeUnit.SECONDS));
                    for (int index = 0; index < 10_000; index++) {
                        long sequence = claim(ledger);
                        if (index % 4 == 0) {
                            ledger.rollbackClaim();
                        } else {
                            accepted.add(sequence);
                        }
                    }
                    return null;
                }));
            }
            results.add(racers.submit(() -> {
                assertTrue(start.await(2, TimeUnit.SECONDS));
                int completed = 0;
                while (completed < 15_000 && !Thread.currentThread().isInterrupted()) {
                    int batch = 0;
                    while (batch < 7 && accepted.poll() != null) {
                        batch++;
                    }
                    if (batch > 0) {
                        ledger.completeBatch(batch);
                        completed += batch;
                    }
                }
                assertEquals(15_000, completed);
                return null;
            }));
            start.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (results.stream().anyMatch(result -> !result.isDone())) {
                assertTrue(ledger.outstanding() >= 0);
                assertTrue(System.nanoTime() < deadline, "并发记账必须在期限内完成");
            }
            for (Future<?> result : results) {
                result.get(1, TimeUnit.SECONDS);
            }
            assertEquals(0, ledger.outstanding());
            assertEquals(19_999, ledger.claimedCursor());
        } finally {
            racers.shutdownNow();
            assertTrue(racers.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void sequenceAndCompletionAtLongMaxValueNeverWrap() throws Exception {
        UnboundedTaskLedger ledger = exhaustedAfterOneMoreClaim();
        assertEquals(Long.MAX_VALUE - 1, claim(ledger));
        assertEquals(1, ledger.outstanding());
        assertThrows(IllegalStateException.class, ledger::candidateSequence);
        assertThrows(IllegalStateException.class,
                () -> ledger.tryCommitClaim(Long.MAX_VALUE));
        assertFalse(ledger.tryCommitClaim(Long.MAX_VALUE - 1));
        assertThrows(IllegalArgumentException.class, () -> ledger.tryCommitClaim(-1));
        ledger.completeBatch(1);
        assertEquals(Long.MAX_VALUE - 1, ledger.claimedCursor());
        assertEquals(0, ledger.outstanding());
        assertThrows(IllegalStateException.class, () -> ledger.completeBatch(1));
        assertThrows(IllegalStateException.class, ledger::rollbackClaim);
    }

    @Test
    void exhaustedQueueRejectsBeforeAttemptingSegmentAllocation() throws Exception {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(1, 0, (id, size) -> {
            assertEquals(0, id, "耗尽后不得再尝试扩段");
            return new UnboundedTaskQueue.Segment(id, size);
        });
        counter(queue.ledger(), "claimed").set(Long.MAX_VALUE);
        assertThrows(IllegalStateException.class, queue::tryClaim);
        assertThrows(IllegalStateException.class, queue::tryClaim);
        assertEquals(Long.MAX_VALUE - 1, queue.claimedCursor());
        assertEquals(new QueueSegmentSnapshot(1, 1), queue.segmentSnapshot());
    }

    @Test
    void rollbackAtLongMaxValueNeverWraps() throws Exception {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        counter(ledger, "claimed").set(Long.MAX_VALUE);
        counter(ledger, "rolledBack").set(Long.MAX_VALUE - 1);
        ledger.rollbackClaim();
        assertEquals(0, ledger.outstanding());
        assertThrows(IllegalStateException.class, ledger::rollbackClaim);
        assertEquals(0, ledger.outstanding());
    }

    static UnboundedTaskLedger exhaustedAfterOneMoreClaim() throws Exception {
        UnboundedTaskLedger ledger = new UnboundedTaskLedger();
        counter(ledger, "claimed").set(Long.MAX_VALUE - 1);
        Field completed = UnboundedTaskLedger.class.getDeclaredField("completed");
        completed.setAccessible(true);
        completed.setLong(ledger, Long.MAX_VALUE - 1);
        return ledger;
    }

    private static AtomicLong counter(UnboundedTaskLedger ledger, String name) throws Exception {
        Field field = UnboundedTaskLedger.class.getDeclaredField(name);
        field.setAccessible(true);
        return (AtomicLong) field.get(ledger);
    }

    private static long claim(UnboundedTaskLedger ledger) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            long candidate = ledger.candidateSequence();
            if (ledger.tryCommitClaim(candidate)) {
                return candidate;
            }
            assertTrue(System.nanoTime() < deadline, "claim 必须取得进展");
        }
    }
}
