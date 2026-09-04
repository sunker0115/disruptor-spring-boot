package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskContextTest {

    @Test
    void emptyContextIsSharedAndImmutable() {
        assertSame(TaskContext.empty(), TaskContext.empty());
        assertFalse(TaskContext.empty().contains(TaskContext.Key.of("trace", String.class)));
        assertThrows(UnsupportedOperationException.class, () -> TaskContext.empty().asMap()
                .put(TaskContext.Key.of("trace", String.class), "value"));
    }

    @Test
    void typedKeysPreserveValueTypesAcrossImmutableCopies() {
        TaskContext.Key<String> trace = TaskContext.Key.of("trace", String.class);
        TaskContext.Key<Integer> attempt = TaskContext.Key.of("attempt", Integer.class);

        TaskContext first = TaskContext.empty().with(trace, "abc");
        TaskContext second = first.with(attempt, 2);

        assertEquals("abc", first.get(trace));
        assertFalse(first.contains(attempt));
        assertEquals("abc", second.get(trace));
        assertEquals(2, second.get(attempt));
        assertThrows(UnsupportedOperationException.class,
                () -> second.asMap().remove(trace));
    }

    @Test
    void copyOfDefensivelyCopiesAndChecksDeclaredValueTypes() {
        TaskContext.Key<String> trace = TaskContext.Key.of("trace", String.class);
        Map<TaskContext.Key<?>, Object> source = new HashMap<>();
        source.put(trace, "before");

        TaskContext context = TaskContext.copyOf(source);
        source.put(trace, "after");

        assertEquals("before", context.get(trace));
        Map<TaskContext.Key<?>, Object> invalid = new HashMap<>();
        invalid.put(trace, 1);
        assertThrows(ClassCastException.class, () -> TaskContext.copyOf(invalid));
    }
}
