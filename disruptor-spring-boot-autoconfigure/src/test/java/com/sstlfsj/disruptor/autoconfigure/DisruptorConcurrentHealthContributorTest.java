package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopModule;
import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DisruptorConcurrentHealthContributorTest {

    @Test
    void reportsLifecycleInfrastructureHealthWithoutTreatingTaskFailureAsDown()
            throws Exception {
        DisruptorEventLoop loop = EventLoopBuilder
                .bounded("health-loop", 8)
                .build();
        DisruptorConcurrentHealthContributor contributor = contributor(loop);

        assertThat(contributor.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(contributor.health().getStatus()).isEqualTo(Status.UP);

        assertThatThrownBy(() -> loop.submit(() -> {
            throw new IllegalArgumentException("business task failed");
        }).get(2, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalArgumentException.class);
        Health afterTaskFailure = contributor.health();
        assertThat(afterTaskFailure.getStatus()).isEqualTo(Status.UP);
        assertThat(afterTaskFailure.getDetails()).containsKey("executors");

        loop.shutdown();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(contributor.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void reportsDownWhenAnyRootHasAnInfrastructureFailure() throws Exception {
        IllegalStateException original = new IllegalStateException("module failed");
        DisruptorEventLoop failing = EventLoopBuilder
                .bounded("health-failure", 8)
                .module(new EventLoopModule() {
                    @Override
                    public void onUpdate(EventLoop loop, long nowNanos) {
                        throw original;
                    }
                })
                .build();
        DisruptorConcurrentHealthContributor contributor = contributor(failing);
        failing.start().toCompletableFuture().get(2, TimeUnit.SECONDS);

        failing.execute(() -> { });
        failing.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);

        Health health = contributor.health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry(
                "failure", original.getClass().getName() + ": " + original.getMessage());
    }

    private static DisruptorConcurrentHealthContributor contributor(
            SupervisedScheduledExecutor<?> executor) {
        return new DisruptorConcurrentHealthContributor(List.of(executor));
    }
}
