package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineSnapshotTest {

    @Test
    void exposesTheDefinedLifecycleAndResultValues() {
        assertEquals(Arrays.asList("NEW", "STARTING", "RUNNING", "QUIESCING", "STOPPING", "TERMINATED"),
                Arrays.stream(PipelineLifecycle.values()).map(Enum::name).toList());
        assertEquals(Arrays.asList("STARTING", "HEALTHY", "UNHEALTHY", "OUT_OF_SERVICE", "TERMINATED"),
                Arrays.stream(PipelineHealth.values()).map(Enum::name).toList());
        assertEquals(Arrays.asList("PUBLISHED", "CAPACITY_EXHAUSTED", "TIMED_OUT", "NOT_RUNNING", "PIPELINE_FAILED"),
                Arrays.stream(PublicationResult.values()).map(Enum::name).toList());
        assertEquals(Arrays.asList("GRACEFUL", "IMMEDIATE"),
                Arrays.stream(ShutdownMode.values()).map(Enum::name).toList());
    }

    @Test
    void derivesHealthForEveryLifecycle() {
        assertHealth(PipelineLifecycle.NEW, null, 0, 0, 0, PipelineHealth.STARTING);
        assertHealth(PipelineLifecycle.STARTING, null, 0, 0, 0, PipelineHealth.STARTING);
        assertHealth(PipelineLifecycle.RUNNING, null, 2, 2, 2, PipelineHealth.HEALTHY);
        assertHealth(PipelineLifecycle.RUNNING, new IllegalStateException("failed"), 2, 2, 2,
                PipelineHealth.UNHEALTHY);
        assertHealth(PipelineLifecycle.RUNNING, null, 0, 0, 0, PipelineHealth.UNHEALTHY);
        assertHealth(PipelineLifecycle.RUNNING, null, 2, 1, 1, PipelineHealth.UNHEALTHY);
        assertHealth(PipelineLifecycle.RUNNING, null, 2, 2, 1, PipelineHealth.UNHEALTHY);
        assertHealth(PipelineLifecycle.QUIESCING, new IllegalStateException("failed"), 2, 2, 2,
                PipelineHealth.OUT_OF_SERVICE);
        assertHealth(PipelineLifecycle.STOPPING, new IllegalStateException("failed"), 2, 2, 2,
                PipelineHealth.OUT_OF_SERVICE);
        assertHealth(PipelineLifecycle.TERMINATED, new IllegalStateException("failed"), 2, 2, 2,
                PipelineHealth.TERMINATED);
    }

    @Test
    void preservesSnapshotFields() {
        IllegalStateException failure = new IllegalStateException("boom");

        PipelineSnapshot snapshot = PipelineSnapshot.builder()
                .name("orders")
                .lifecycle(PipelineLifecycle.RUNNING)
                .acceptingPublications(true)
                .expectedConsumers(3)
                .createdConsumers(3)
                .aliveConsumers(3)
                .bufferSize(1024)
                .backlog(12)
                .failure(failure)
                .gracefulTermination(false)
                .build();

        assertEquals("orders", snapshot.name());
        assertEquals(PipelineLifecycle.RUNNING, snapshot.lifecycle());
        assertEquals(PipelineHealth.UNHEALTHY, snapshot.health());
        assertTrue(snapshot.acceptingPublications());
        assertEquals(3, snapshot.expectedConsumers());
        assertEquals(3, snapshot.createdConsumers());
        assertEquals(3, snapshot.aliveConsumers());
        assertEquals(1024, snapshot.bufferSize());
        assertEquals(12, snapshot.backlog());
        assertSame(failure, snapshot.failure());
        assertFalse(snapshot.gracefulTermination());

        assertNull(PipelineSnapshot.builder()
                .name("orders")
                .lifecycle(PipelineLifecycle.NEW)
                .acceptingPublications(false)
                .expectedConsumers(0)
                .createdConsumers(0)
                .aliveConsumers(0)
                .bufferSize(0)
                .backlog(0)
                .gracefulTermination(false)
                .build()
                .failure());
    }

    @Test
    void rejectsInvalidSnapshotArguments() {
        assertThrows(NullPointerException.class, () -> newSnapshotBuilder().name(null).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder().name(" ").build());
        assertThrows(NullPointerException.class, () -> newSnapshotBuilder().lifecycle(null).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder().expectedConsumers(-1).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .expectedConsumers(1).createdConsumers(-1).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .expectedConsumers(1).createdConsumers(1).aliveConsumers(-1).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder().bufferSize(-1).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder().backlog(-1).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .expectedConsumers(1).createdConsumers(2).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .expectedConsumers(1).createdConsumers(1).aliveConsumers(2).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .lifecycle(PipelineLifecycle.STARTING).acceptingPublications(true).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .lifecycle(PipelineLifecycle.STOPPING).gracefulTermination(true).build());
    }

    @Test
    void canonicalConstructorRejectsInvalidHealth() {
        assertThrows(NullPointerException.class, () -> new PipelineSnapshot(
                "orders", PipelineLifecycle.NEW, null, false, 0, 0, 0, 0, 0, null, false));
        assertThrows(IllegalArgumentException.class, () -> new PipelineSnapshot(
                "orders", PipelineLifecycle.NEW, PipelineHealth.HEALTHY,
                false, 0, 0, 0, 0, 0, null, false));
    }

    private static void assertHealth(
            PipelineLifecycle lifecycle,
            Throwable failure,
            int expectedConsumers,
            int createdConsumers,
            int aliveConsumers,
            PipelineHealth expectedHealth) {
        PipelineSnapshot snapshot = PipelineSnapshot.builder()
                .name("orders")
                .lifecycle(lifecycle)
                .acceptingPublications(false)
                .expectedConsumers(expectedConsumers)
                .createdConsumers(createdConsumers)
                .aliveConsumers(aliveConsumers)
                .bufferSize(1024)
                .backlog(0)
                .failure(failure)
                .gracefulTermination(lifecycle == PipelineLifecycle.TERMINATED)
                .build();

        assertEquals(expectedHealth, snapshot.health());
    }

    private static PipelineSnapshot.PipelineSnapshotBuilder newSnapshotBuilder() {
        return PipelineSnapshot.builder()
                .name("orders")
                .lifecycle(PipelineLifecycle.NEW)
                .acceptingPublications(false)
                .expectedConsumers(0)
                .createdConsumers(0)
                .aliveConsumers(0)
                .bufferSize(0)
                .backlog(0)
                .gracefulTermination(false);
    }
}
