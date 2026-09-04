package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.NanoClock;
import com.sstlfsj.disruptor.concurrent.ScheduledTaskSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexedScheduledHeapTest {

    private static final NanoClock CLOCK = () -> 0;

    @Test
    void ordersByTriggerThenPriorityThenAcceptedSequence() {
        IndexedScheduledHeap heap = new IndexedScheduledHeap();
        ScheduledTask<Void> later = task(1, 20, 100);
        ScheduledTask<Void> lowPriority = task(2, 10, 1);
        ScheduledTask<Void> highPriorityLaterSequence = task(4, 10, 5);
        ScheduledTask<Void> highPriorityEarlierSequence = task(3, 10, 5);

        heap.add(later);
        heap.add(lowPriority);
        heap.add(highPriorityLaterSequence);
        heap.add(highPriorityEarlierSequence);

        assertEquals(highPriorityEarlierSequence, heap.poll());
        assertEquals(highPriorityLaterSequence, heap.poll());
        assertEquals(lowPriority, heap.poll());
        assertEquals(later, heap.poll());
        assertNull(heap.poll());
    }

    @Test
    void arbitraryRemovalPreservesHeapOrderAndClearsIndex() {
        IndexedScheduledHeap heap = new IndexedScheduledHeap();
        ScheduledTask<Void> first = task(1, 1, 0);
        ScheduledTask<Void> removed = task(2, 2, 0);
        ScheduledTask<Void> third = task(3, 3, 0);
        ScheduledTask<Void> fourth = task(4, 4, 0);
        heap.add(fourth);
        heap.add(removed);
        heap.add(third);
        heap.add(first);

        assertTrue(heap.remove(removed));
        assertFalse(heap.remove(removed));
        assertEquals(-1, removed.heapIndex());
        assertEquals(first, heap.poll());
        assertEquals(third, heap.poll());
        assertEquals(fourth, heap.poll());
    }

    private static ScheduledTask<Void> task(long sequence, long trigger, int priority) {
        return new ScheduledTask<>(sequence, ScheduledTaskSpec.<Void>builder()
                .task(context -> null)
                .triggerAfter(Duration.ofNanos(trigger))
                .priority(priority)
                .build(), 0, CLOCK);
    }
}
