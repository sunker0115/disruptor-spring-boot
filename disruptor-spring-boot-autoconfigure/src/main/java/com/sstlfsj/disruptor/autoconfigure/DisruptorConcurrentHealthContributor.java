package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.concurrent.EventLoopGroupSnapshot;
import com.sstlfsj.disruptor.concurrent.EventLoopSnapshot;
import com.sstlfsj.disruptor.concurrent.SupervisedScheduledExecutor;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 基于不可变快照聚合根 EventLoop 与 EventLoopGroup 的基础设施健康。 */
public final class DisruptorConcurrentHealthContributor implements HealthIndicator {

    private final List<SupervisedScheduledExecutor<?>> roots;

    public DisruptorConcurrentHealthContributor(
            List<SupervisedScheduledExecutor<?>> roots) {
        this.roots = List.copyOf(Objects.requireNonNull(roots, "roots 不能为空"));
        if (this.roots.isEmpty()) {
            throw new IllegalArgumentException("roots 不能为空");
        }
    }

    @Override
    public Health health() {
        List<RootHealth> states = roots.stream()
                .map(DisruptorConcurrentHealthContributor::state)
                .toList();
        Throwable firstFailure = states.stream()
                .map(RootHealth::failure)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        boolean allAvailable = states.stream().allMatch(root ->
                root.lifecycle() == SupervisedLifecycle.RUNNING && root.accepting());

        Health.Builder health = firstFailure != null
                ? Health.down()
                : allAvailable ? Health.up() : Health.outOfService();
        health.withDetail("executors", details(states));
        if (firstFailure != null) {
            health.withDetail("failure", firstFailure.getClass().getName()
                    + ": " + firstFailure.getMessage());
        }
        return health.build();
    }

    private static RootHealth state(SupervisedScheduledExecutor<?> root) {
        Object snapshot = root.snapshot();
        if (snapshot instanceof EventLoopSnapshot loop) {
            return new RootHealth(
                    loop.name(), loop.lifecycle(), loop.acceptingTasks(), loop.failure());
        }
        if (snapshot instanceof EventLoopGroupSnapshot group) {
            return new RootHealth(
                    group.name(), group.lifecycle(), group.acceptingTasks(), group.failure());
        }
        throw new IllegalStateException("不支持的 concurrent 根快照类型："
                + snapshot.getClass().getName());
    }

    private static List<Map<String, Object>> details(List<RootHealth> states) {
        List<Map<String, Object>> result = new ArrayList<>(states.size());
        for (RootHealth state : states) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("name", state.name());
            detail.put("lifecycle", state.lifecycle().name());
            detail.put("accepting", state.accepting());
            if (state.failure() != null) {
                detail.put("failure", state.failure().getClass().getName()
                        + ": " + state.failure().getMessage());
            }
            result.add(Map.copyOf(detail));
        }
        return List.copyOf(result);
    }

    private record RootHealth(
            String name,
            SupervisedLifecycle lifecycle,
            boolean accepting,
            Throwable failure) {
    }
}
