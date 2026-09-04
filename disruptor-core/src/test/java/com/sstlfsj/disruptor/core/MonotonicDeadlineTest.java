package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MonotonicDeadlineTest {

    @Test
    void measuresElapsedTimeAcrossPositiveAdditionWraparound() {
        AtomicLong clock = new AtomicLong(Long.MAX_VALUE - 5L);
        MonotonicDeadline deadline = MonotonicDeadline.after(
                Duration.ofNanos(10L), clock::get);

        assertEquals(10L, deadline.remainingNanos());
        clock.set(Long.MIN_VALUE + 1L);
        assertEquals(3L, deadline.remainingNanos());
        assertFalse(deadline.isExpired());
        clock.set(Long.MIN_VALUE + 4L);
        assertEquals(0L, deadline.remainingNanos());
        assertTrue(deadline.isExpired());
    }

    @Test
    void acceptsNegativeNanoTimeOrigin() {
        AtomicLong clock = new AtomicLong(-20L);
        MonotonicDeadline deadline = MonotonicDeadline.after(
                Duration.ofNanos(8L), clock::get);

        clock.set(-15L);
        assertEquals(3L, deadline.remainingNanos());
        assertFalse(deadline.isExpired());
    }

    @Test
    void preservesEntireSaturatedDurationBudget() {
        AtomicLong clock = new AtomicLong(100L);
        MonotonicDeadline deadline = MonotonicDeadline.after(
                Duration.ofSeconds(Long.MAX_VALUE), clock::get);

        assertEquals(Long.MAX_VALUE, deadline.remainingNanos());
        clock.incrementAndGet();
        assertEquals(Long.MAX_VALUE - 1L, deadline.remainingNanos());
        assertFalse(deadline.isExpired());
    }
}
