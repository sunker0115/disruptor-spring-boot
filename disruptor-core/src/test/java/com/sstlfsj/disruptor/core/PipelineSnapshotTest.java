package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void derivesHealthFromSealedStartedAndAliveFacts() {
        PipelineSnapshot running = runningSnapshotBuilder().build();
        assertEquals(PipelineHealth.HEALTHY, running.health());

        PipelineSnapshot stopping = runningSnapshotBuilder()
                .lifecycle(PipelineLifecycle.STOPPING)
                .acceptingPublications(false)
                .shutdownMode(ShutdownMode.IMMEDIATE)
                .aliveConsumers(1)
                .build();
        assertEquals(PipelineHealth.OUT_OF_SERVICE, stopping.health());

        PipelineSnapshot terminated = stopping.toBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED)
                .aliveConsumers(0)
                .build();
        assertEquals(PipelineHealth.TERMINATED, terminated.health());
    }

    @Test
    void preservesFailureIdentityAndDerivesGracefulTerminationFromHistory() {
        IllegalStateException failure = new IllegalStateException("boom");
        PipelineSnapshot failed = runningSnapshotBuilder()
                .lifecycle(PipelineLifecycle.STOPPING)
                .acceptingPublications(false)
                .shutdownMode(ShutdownMode.IMMEDIATE)
                .failure(failure)
                .build();
        assertSame(failure, failed.failure());
        assertFalse(failed.gracefulTermination());

        PipelineSnapshot graceful = runningSnapshotBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED)
                .acceptingPublications(false)
                .aliveConsumers(0)
                .shutdownMode(ShutdownMode.GRACEFUL)
                .drainCommitted(true)
                .gracefulStopApplied(true)
                .build();
        assertTrue(graceful.gracefulTermination());
    }

    @Test
    void rejectsCountLifecycleAndGracefulHistoryContradictions() {
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .createdConsumers(1).startedConsumers(2).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .createdConsumers(1).startedConsumers(1).aliveConsumers(2).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .registrationSealed(true).expectedConsumers(2).createdConsumers(1).build());
        assertThrows(IllegalArgumentException.class, () -> runningSnapshotBuilder()
                .startedConsumers(1).aliveConsumers(1).build());
        assertThrows(IllegalArgumentException.class, () -> runningSnapshotBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED).aliveConsumers(1).build());
        assertThrows(IllegalArgumentException.class, () -> runningSnapshotBuilder()
                .lifecycle(PipelineLifecycle.TERMINATED)
                .acceptingPublications(false)
                .registrationSealed(false)
                .expectedConsumers(0)
                .aliveConsumers(0)
                .shutdownMode(ShutdownMode.IMMEDIATE)
                .build());
        assertThrows(IllegalArgumentException.class, () -> runningSnapshotBuilder()
                .drainCommitted(true).build());
        assertThrows(IllegalArgumentException.class, () -> newSnapshotBuilder()
                .reachedRunning(false).drainCommitted(true).build());
    }

    private static PipelineSnapshot.PipelineSnapshotBuilder runningSnapshotBuilder() {
        return PipelineSnapshot.builder()
                .name("orders")
                .lifecycle(PipelineLifecycle.RUNNING)
                .acceptingPublications(true)
                .registrationSealed(true)
                .expectedConsumers(2)
                .createdConsumers(2)
                .startedConsumers(2)
                .aliveConsumers(2)
                .bufferSize(16)
                .backlog(0)
                .reachedRunning(true);
    }

    private static PipelineSnapshot.PipelineSnapshotBuilder newSnapshotBuilder() {
        return PipelineSnapshot.builder()
                .name("orders")
                .lifecycle(PipelineLifecycle.NEW)
                .acceptingPublications(false)
                .expectedConsumers(0)
                .createdConsumers(0)
                .startedConsumers(0)
                .aliveConsumers(0)
                .bufferSize(0)
                .backlog(0);
    }
}
