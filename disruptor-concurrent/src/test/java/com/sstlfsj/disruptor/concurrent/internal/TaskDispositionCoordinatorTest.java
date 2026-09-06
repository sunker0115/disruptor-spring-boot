package com.sstlfsj.disruptor.concurrent.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskDispositionCoordinatorTest {

    @Test
    void returningHasOneOwnerAndCompletesWithoutAllowingDiscard() {
        TaskDispositionCoordinator coordinator = new TaskDispositionCoordinator();
        assertEquals(TaskDispositionCoordinator.State.NONE, coordinator.state());
        assertFalse(coordinator.isTerminal());
        assertTrue(coordinator.tryBeginReturning());
        assertFalse(coordinator.tryBeginReturning());
        assertFalse(coordinator.tryBeginDiscarding());
        assertFalse(coordinator.isTerminal());

        coordinator.completeReturned();

        assertEquals(TaskDispositionCoordinator.State.RETURNED, coordinator.state());
        assertTrue(coordinator.isTerminal());
        assertFalse(coordinator.tryBeginDiscarding());
    }

    @Test
    void discardingCompletesIdempotentlyWithoutAllowingReturn() {
        TaskDispositionCoordinator coordinator = new TaskDispositionCoordinator();
        assertTrue(coordinator.tryBeginDiscarding());
        assertFalse(coordinator.tryBeginReturning());
        assertFalse(coordinator.tryBeginDiscarding());
        assertFalse(coordinator.isTerminal());

        coordinator.completeDiscarded();
        coordinator.completeDiscarded();

        assertEquals(TaskDispositionCoordinator.State.DISCARDED, coordinator.state());
        assertTrue(coordinator.isTerminal());
    }

    @Test
    void failedReturningTransfersOwnershipToDiscarding() {
        TaskDispositionCoordinator coordinator = new TaskDispositionCoordinator();
        assertTrue(coordinator.tryBeginReturning());

        coordinator.failReturningToDiscarding(new IllegalStateException("返回扫描失败"));

        assertEquals(TaskDispositionCoordinator.State.DISCARDING, coordinator.state());
        assertFalse(coordinator.isTerminal());
        assertFalse(coordinator.tryBeginReturning());
        coordinator.completeDiscarded();
        assertTrue(coordinator.isTerminal());
    }

    @Test
    void racingReturnAndDiscardHaveExactlyOneWinner() throws Exception {
        try (var callers = Executors.newFixedThreadPool(2)) {
            for (int attempt = 0; attempt < 100; attempt++) {
                TaskDispositionCoordinator coordinator = new TaskDispositionCoordinator();
                CountDownLatch start = new CountDownLatch(1);
                var returning = callers.submit(() -> {
                    start.await();
                    return coordinator.tryBeginReturning();
                });
                var discarding = callers.submit(() -> {
                    start.await();
                    return coordinator.tryBeginDiscarding();
                });
                start.countDown();

                assertTrue(returning.get(2, TimeUnit.SECONDS)
                        ^ discarding.get(2, TimeUnit.SECONDS));
            }
        }
    }
}
