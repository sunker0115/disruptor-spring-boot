package com.sstlfsj.disruptor.concurrent.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskQueueContractTest {

    private static final int PRODUCERS = 4;
    private static final int TASKS_PER_PRODUCER = 1_000;

    static Stream<QueueFactory> queues() {
        return Stream.of(
                new QueueFactory("bounded", () -> new BoundedTaskQueue(4_096)),
                new QueueFactory("unbounded", () -> new UnboundedTaskQueue(64, 8)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void pendingCannotBecomeNegativeWhenConsumerAdvancesBetweenCursorReads(
            QueueFactory factory) throws Exception {
        TaskQueue queue = factory.create();
        CountDownLatch claimedRead = new CountDownLatch(1);
        CountDownLatch consumed = new CountDownLatch(1);
        TaskQueue observed = (TaskQueue) Proxy.newProxyInstance(
                TaskQueue.class.getClassLoader(), new Class<?>[]{TaskQueue.class},
                (proxy, method, arguments) -> {
                    if (method.isDefault()) {
                        return InvocationHandler.invokeDefault(proxy, method, arguments);
                    }
                    Object result = method.invoke(queue, arguments);
                    if (method.getName().equals("claimedCursor")) {
                        claimedRead.countDown();
                        assertTrue(consumed.await(2, TimeUnit.SECONDS));
                    }
                    return result;
                });
        try (ExecutorService observer = Executors.newSingleThreadExecutor()) {
            Future<Long> pending = observer.submit(observed::pending);
            try {
                assertTrue(claimedRead.await(2, TimeUnit.SECONDS));
                publishOrdinary(queue);
                assertTrue(queue.poll());
                finishCurrentOrdinary(queue);
            } finally {
                consumed.countDown();
            }
            assertEquals(0, pending.get(2, TimeUnit.SECONDS));
        }
        assertEquals(0, queue.pending());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void consumerStopsAtTheFirstPublicationHole(QueueFactory factory) {
        TaskQueue queue = factory.create();
        long first = queue.tryClaim();
        long second = queue.tryClaim();
        SequencedRunnable firstTask = new SequencedRunnable(first);
        SequencedRunnable secondTask = new SequencedRunnable(second);
        queue.writeOrdinary(second, secondTask);
        queue.publish(second);

        assertFalse(queue.poll());

        queue.writeOrdinary(first, firstTask);
        queue.publish(first);
        assertTrue(queue.poll());
        assertSame(firstTask, queue.currentOrdinary());
        finishCurrentOrdinary(queue);
        assertTrue(queue.poll());
        assertSame(secondTask, queue.currentOrdinary());
        finishCurrentOrdinary(queue);
        assertFalse(queue.poll());
        assertEquals(0, queue.pending());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void ordinaryOwnershipMovesFromWaitingToRunningToTerminal(QueueFactory factory) {
        TaskQueue queue = factory.create();
        publishOrdinary(queue);

        assertTrue(queue.poll());
        assertEquals(TaskType.ORDINARY, queue.currentType());
        assertEquals(OrdinaryState.WAITING, queue.currentOrdinaryState());
        assertTrue(queue.tryStartCurrentOrdinary());
        assertFalse(queue.tryStartCurrentOrdinary());
        assertEquals(OrdinaryState.RUNNING, queue.currentOrdinaryState());
        Runnable captured = queue.currentOrdinary();
        queue.advanceConsumer();
        queue.terminalizeCurrentOrdinary();
        assertEquals(OrdinaryState.TERMINAL, queue.currentOrdinaryState());
        queue.releaseCurrentSlot();
        assertTrue(captured instanceof SequencedRunnable);
    }

    @Test
    void boundedSlotCannotBeReusedUntilItIsReleased() {
        BoundedTaskQueue queue = new BoundedTaskQueue(1);
        publishOrdinary(queue);
        assertEquals(-1, queue.tryClaim());

        assertTrue(queue.poll());
        assertTrue(queue.tryStartCurrentOrdinary());
        queue.advanceConsumer();
        assertEquals(0, queue.pending());
        assertEquals(-1, queue.tryClaim());

        queue.terminalizeCurrentOrdinary();
        queue.releaseCurrentSlot();
        assertEquals(1, queue.tryClaim());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void concurrentProducersPublishEverySequenceExactlyOnce(QueueFactory factory) throws Exception {
        TaskQueue queue = factory.create();
        ExecutorService threads = Executors.newFixedThreadPool(PRODUCERS + 1);
        CountDownLatch start = new CountDownLatch(1);
        try {
            int total = PRODUCERS * TASKS_PER_PRODUCER;
            Future<?> consumer = threads.submit(() -> {
                start.await();
                long expected = 0;
                while (expected < total) {
                    if (!queue.poll()) {
                        Thread.onSpinWait();
                        continue;
                    }
                    assertEquals(TaskType.ORDINARY, queue.currentType());
                    SequencedRunnable task = (SequencedRunnable) queue.currentOrdinary();
                    assertEquals(expected, task.sequence());
                    assertTrue(queue.tryStartCurrentOrdinary());
                    queue.advanceConsumer();
                    queue.terminalizeCurrentOrdinary();
                    queue.releaseCurrentSlot();
                    expected++;
                }
                return null;
            });
            List<Future<?>> producers = new ArrayList<>();
            for (int producer = 0; producer < PRODUCERS; producer++) {
                producers.add(threads.submit(() -> {
                    start.await();
                    for (int index = 0; index < TASKS_PER_PRODUCER; index++) {
                        long sequence;
                        while ((sequence = queue.tryClaim()) < 0) {
                            Thread.onSpinWait();
                        }
                        queue.writeOrdinary(sequence, new SequencedRunnable(sequence));
                        queue.publish(sequence);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> producer : producers) {
                producer.get(10, TimeUnit.SECONDS);
            }
            consumer.get(10, TimeUnit.SECONDS);

            assertFalse(queue.poll());
            assertEquals(total - 1L, queue.claimedCursor());
            assertEquals(0, queue.pending());
            assertEquals(0, retainedReferences(queue));
        } finally {
            threads.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void tombstoneOccupiesItsSequenceWithoutExposingAPayload(QueueFactory factory) {
        TaskQueue queue = factory.create();
        long tombstone = queue.tryClaim();
        queue.writeTombstone(tombstone);
        queue.publish(tombstone);
        SequencedRunnable ordinary = publishOrdinary(queue);

        assertTrue(queue.poll());
        assertEquals(TaskType.TOMBSTONE, queue.currentType());
        queue.advanceConsumer();
        queue.releaseCurrentSlot();
        assertTrue(queue.poll());
        assertSame(ordinary, queue.currentOrdinary());
        finishCurrentOrdinary(queue);
        assertFalse(queue.poll());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queues")
    void scannerAndWorkerCannotBothClaimTheSameOrdinary(QueueFactory factory) throws Exception {
        TaskQueue queue = factory.create();
        SequencedRunnable task = publishOrdinary(queue);
        assertTrue(queue.poll());
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean workerWon = new AtomicBoolean();
        List<Runnable> returned = new ArrayList<>();
        Thread scanner = Thread.ofPlatform().start(() -> {
            await(start);
            queue.scanOrdinaryUnstarted(queue.claimedCursor(), OrdinaryDisposition.RETURN,
                    (sequence, original) -> returned.add(original));
        });

        start.countDown();
        workerWon.set(queue.tryStartCurrentOrdinary());
        scanner.join(1_000);
        assertFalse(scanner.isAlive());
        assertEquals(workerWon.get() ? 0 : 1, returned.size());
        if (!workerWon.get()) {
            assertSame(task, returned.get(0));
            assertEquals(OrdinaryState.RETURNED, queue.currentOrdinaryState());
        }
        queue.advanceConsumer();
        if (workerWon.get()) {
            queue.terminalizeCurrentOrdinary();
        }
        queue.releaseCurrentSlot();
    }

    @Test
    void unboundedQueueRecyclesSegmentsWithoutIncreasingAllocatedCount() {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(2, 8);
        for (int index = 0; index < 6; index++) {
            publishOrdinary(queue);
        }
        assertEquals(new QueueSegmentSnapshot(3, 3), queue.segmentSnapshot());
        consumeOrdinaries(queue, 6);
        assertEquals(new QueueSegmentSnapshot(3, 1), queue.segmentSnapshot());

        for (int index = 0; index < 4; index++) {
            publishOrdinary(queue);
        }
        assertEquals(new QueueSegmentSnapshot(3, 3), queue.segmentSnapshot());
        consumeOrdinaries(queue, 4);
    }

    @Test
    void unboundedReusedCellRequiresTheNewPublicationGeneration() {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(1, 8);
        publishOrdinary(queue);
        consumeOrdinaries(queue, 1);
        publishOrdinary(queue);
        consumeOrdinaries(queue, 1);

        long reused = queue.tryClaim();
        SequencedRunnable task = new SequencedRunnable(reused);
        queue.writeOrdinary(reused, task);
        assertFalse(queue.poll());

        queue.publish(reused);
        assertTrue(queue.poll());
        assertSame(task, queue.currentOrdinary());
        finishCurrentOrdinary(queue);
    }

    @Test
    void unboundedDelayedProducerLookupIsProtectedBySegmentLifecycleLock() throws Exception {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(1, 8);
        publishOrdinary(queue);
        long delayed = queue.tryClaim();
        SequencedRunnable delayedTask = new SequencedRunnable(delayed);
        SequencedRunnable aheadTask = publishOrdinary(queue);
        ReentrantLock lifecycleLock = segmentLifecycleLock(queue);
        ExecutorService producer = Executors.newSingleThreadExecutor();
        CountDownLatch writeStarted = new CountDownLatch(1);
        try {
            Future<?> delayedWrite;
            lifecycleLock.lock();
            try {
                delayedWrite = producer.submit(() -> {
                    writeStarted.countDown();
                    queue.writeOrdinary(delayed, delayedTask);
                });
                assertTrue(writeStarted.await(1, TimeUnit.SECONDS));
                Future<?> blockedWrite = delayedWrite;
                assertThrows(TimeoutException.class,
                        () -> blockedWrite.get(200, TimeUnit.MILLISECONDS));
            } finally {
                lifecycleLock.unlock();
            }
            delayedWrite.get(1, TimeUnit.SECONDS);
            queue.publish(delayed);
            consumeOrdinaries(queue, 3);
            assertEquals(2, aheadTask.sequence());
        } finally {
            producer.shutdownNow();
        }
    }

    @ParameterizedTest(name = "producers = {0}")
    @ValueSource(ints = {1, 2, 4, 8})
    void reversePublicationAcrossSegmentsSurvivesHeadAdvanceAndReuse(int producerCount)
            throws Exception {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(4, 8);
        ExecutorService producers = Executors.newFixedThreadPool(producerCount);
        try {
            for (int round = 0; round < 16; round++) {
                // 已发布前缀允许 head 前进；下一段首槽由落后 producer 持有。
                long prefix = queue.claimedCursor() + 1;
                for (int index = 0; index < 4; index++) {
                    publishOrdinary(queue);
                }
                long hole = prefix + 4;
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch laterPublished = new CountDownLatch(producerCount);
                CountDownLatch resumeHole = new CountDownLatch(1);
                List<Future<?>> publications = new ArrayList<>();
                for (int producer = 0; producer < producerCount; producer++) {
                    publications.add(producers.submit(() -> {
                        assertTrue(start.await(2, TimeUnit.SECONDS));
                        long[] claims = new long[8];
                        for (int index = 0; index < claims.length; index++) {
                            claims[index] = queue.tryClaim();
                        }
                        boolean holdsHole = false;
                        for (int index = claims.length - 1; index >= 0; index--) {
                            long sequence = claims[index];
                            if (sequence == hole) {
                                holdsHole = true;
                            } else {
                                queue.writeOrdinary(sequence, new SequencedRunnable(sequence));
                                queue.publish(sequence);
                            }
                        }
                        laterPublished.countDown();
                        if (holdsHole) {
                            assertTrue(resumeHole.await(2, TimeUnit.SECONDS));
                            queue.writeOrdinary(hole, new SequencedRunnable(hole));
                            queue.publish(hole);
                        }
                        return null;
                    }));
                }
                start.countDown();
                try {
                    assertTrue(laterPublished.await(2, TimeUnit.SECONDS));
                    consumeOrdinaries(queue, 4);
                    assertFalse(queue.poll(), "不能越过已跨段发布的首个 hole");
                    assertEquals(hole - 1, queue.consumerCursor());
                    assertEquals(producerCount * 2, queue.segmentSnapshot().active(),
                            "head 必须已越过前缀段，落后 producer 的目标段仍在链上");
                } finally {
                    resumeHole.countDown();
                }
                for (Future<?> publication : publications) {
                    publication.get(2, TimeUnit.SECONDS);
                }
                for (long sequence = hole; sequence < hole + producerCount * 8L; sequence++) {
                    assertTrue(queue.poll());
                    assertEquals(sequence, ((SequencedRunnable) queue.currentOrdinary()).sequence());
                    assertEquals(OrdinaryState.WAITING, queue.currentOrdinaryState());
                    finishCurrentOrdinary(queue);
                }
                assertFalse(queue.poll());
                assertEquals(0, queue.pending());
                assertEquals(0, queue.retainedReferences());
                assertEquals(1, queue.segmentSnapshot().active());
                assertTrue(queue.segmentSnapshot().allocated() <= 9);
            }
        } finally {
            producers.shutdownNow();
            assertTrue(producers.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @EnumSource(value = OrdinaryState.class, names = {"RETURNED", "DISCARDED", "TERMINAL"})
    void reusedGenerationCannotExposeOldOwnershipOrPayload(OrdinaryState oldState) {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(1, 1);
        for (int generation = 0; generation < 16; generation++) {
            long sequence = queue.tryClaim();
            assertFalse(queue.poll(), "旧代 publication 不得冒充本代");
            SequencedRunnable task = new SequencedRunnable(sequence);
            queue.writeOrdinary(sequence, task);
            assertFalse(queue.poll(), "写入 payload 不等于发布");
            queue.publish(sequence);
            if (oldState != OrdinaryState.TERMINAL) {
                List<Runnable> claimed = new ArrayList<>();
                queue.scanOrdinaryUnstarted(sequence,
                        oldState == OrdinaryState.RETURNED
                                ? OrdinaryDisposition.RETURN : OrdinaryDisposition.DISCARD,
                        (claimedSequence, original) -> {
                            assertEquals(sequence, claimedSequence);
                            claimed.add(original);
                        });
                assertEquals(List.of(task), claimed);
            }
            assertTrue(queue.poll());
            assertSame(task, queue.currentOrdinary());
            if (oldState == OrdinaryState.TERMINAL) {
                assertTrue(queue.tryStartCurrentOrdinary());
                queue.terminalizeCurrentOrdinary();
            } else {
                assertFalse(queue.tryStartCurrentOrdinary());
            }
            assertEquals(oldState, queue.currentOrdinaryState());
            queue.advanceConsumer();
            queue.releaseCurrentSlot();
            assertFalse(queue.poll());
            assertEquals(0, queue.retainedReferences());
            assertTrue(queue.segmentSnapshot().allocated() <= 2,
                    "固定池必须在多代间重复使用段");
        }
    }

    @Test
    void unboundedDoubleScannerAndConsumerDoNotUseRecycledSegments() throws Exception {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(2, 8);
        int total = 128;
        for (int index = 0; index < total; index++) {
            publishOrdinary(queue);
        }
        long frozen = queue.claimedCursor();
        Set<Long> claimed = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);
        Runnable scan = () -> {
            await(start);
            queue.scanOrdinaryUnstarted(frozen, OrdinaryDisposition.RETURN,
                    (sequence, task) -> assertTrue(claimed.add(sequence)));
        };
        ExecutorService racers = Executors.newFixedThreadPool(3);
        try {
            Future<?> firstScanner = racers.submit(scan);
            Future<?> secondScanner = racers.submit(scan);
            Future<?> consumer = racers.submit(() -> {
                await(start);
                int consumed = 0;
                while (consumed < total) {
                    if (!queue.poll()) {
                        Thread.onSpinWait();
                        continue;
                    }
                    long sequence = ((SequencedRunnable) queue.currentOrdinary()).sequence();
                    if (queue.tryStartCurrentOrdinary()) {
                        assertTrue(claimed.add(sequence));
                    }
                    OrdinaryState state = queue.currentOrdinaryState();
                    queue.advanceConsumer();
                    if (state == OrdinaryState.RUNNING) {
                        queue.terminalizeCurrentOrdinary();
                    }
                    queue.releaseCurrentSlot();
                    consumed++;
                }
            });

            start.countDown();
            firstScanner.get(5, TimeUnit.SECONDS);
            secondScanner.get(5, TimeUnit.SECONDS);
            consumer.get(5, TimeUnit.SECONDS);
            assertEquals(total, claimed.size());
            QueueSegmentSnapshot segments = queue.segmentSnapshot();
            assertEquals(1, segments.active());
            assertTrue(segments.allocated() <= 9);
        } finally {
            racers.shutdownNow();
        }
    }

    @Test
    void unboundedSegmentSnapshotStaysConsistentDuringGrowthAndRecycle()
            throws Exception {
        UnboundedTaskQueue queue = new UnboundedTaskQueue(1, 0);
        int total = 10_000;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService racers = Executors.newFixedThreadPool(2);
        try {
            Future<?> producer = racers.submit(() -> {
                await(start);
                for (int index = 0; index < total; index++) {
                    publishOrdinary(queue);
                }
            });
            Future<?> consumer = racers.submit(() -> {
                await(start);
                int consumed = 0;
                while (consumed < total && !Thread.currentThread().isInterrupted()) {
                    if (!queue.poll()) {
                        Thread.onSpinWait();
                        continue;
                    }
                    finishCurrentOrdinary(queue);
                    consumed++;
                }
                assertEquals(total, consumed);
            });

            start.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!producer.isDone() || !consumer.isDone()) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new AssertionError("并发 segment 扩缩未在期限内完成");
                }
                QueueSegmentSnapshot snapshot = queue.segmentSnapshot();
                assertTrue(snapshot.active() >= 1);
                assertTrue(snapshot.active() <= snapshot.allocated());
            }
            producer.get(5, TimeUnit.SECONDS);
            consumer.get(5, TimeUnit.SECONDS);
        } finally {
            racers.shutdownNow();
            racers.awaitTermination(2, TimeUnit.SECONDS);
        }
        QueueSegmentSnapshot drained = queue.segmentSnapshot();
        assertEquals(1, drained.active());
        assertEquals(1, drained.allocated());
    }

    private static SequencedRunnable publishOrdinary(TaskQueue queue) {
        long sequence = queue.tryClaim();
        assertTrue(sequence >= 0);
        SequencedRunnable task = new SequencedRunnable(sequence);
        queue.writeOrdinary(sequence, task);
        queue.publish(sequence);
        return task;
    }

    private static void consumeOrdinaries(TaskQueue queue, int count) {
        for (int index = 0; index < count; index++) {
            assertTrue(queue.poll());
            finishCurrentOrdinary(queue);
        }
    }

    private static void finishCurrentOrdinary(TaskQueue queue) {
        assertTrue(queue.tryStartCurrentOrdinary());
        queue.advanceConsumer();
        queue.terminalizeCurrentOrdinary();
        queue.releaseCurrentSlot();
    }

    private static int retainedReferences(TaskQueue queue) {
        if (queue instanceof BoundedTaskQueue bounded) {
            return bounded.retainedReferences();
        }
        return ((UnboundedTaskQueue) queue).retainedReferences();
    }

    private static ReentrantLock segmentLifecycleLock(UnboundedTaskQueue queue)
            throws ReflectiveOperationException {
        Field field = UnboundedTaskQueue.class.getDeclaredField("segmentLock");
        field.setAccessible(true);
        return (ReentrantLock) field.get(queue);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private record SequencedRunnable(long sequence) implements Runnable {
        @Override
        public void run() {
        }
    }

    private record QueueFactory(String name, Factory factory) {
        TaskQueue create() {
            return factory.create();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @FunctionalInterface
    private interface Factory {
        TaskQueue create();
    }
}
