package com.sstlfsj.disruptor.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单条管道的处理阶段拓扑：由阶段依赖图构建，校验依赖存在性与无环，给出拓扑顺序
 * （上游在前）与叶子阶段集合。纯逻辑，不依赖 Spring / Disruptor，便于单元测试。
 */
public final class PipelineTopology {

    private final Map<String, List<String>> dependencies;
    private final List<String> order;

    private PipelineTopology(Map<String, List<String>> dependencies, List<String> order) {
        this.dependencies = dependencies;
        this.order = order;
    }

    /**
     * @param stageAfter 各阶段声明的 after 依赖（key 为阶段名）
     * @return 校验并拓扑排序后的拓扑
     * @throws IllegalStateException 依赖引用了不存在的阶段，或存在循环依赖
     */
    public static PipelineTopology build(Map<String, List<String>> stageAfter) {
        Map<String, List<String>> deps = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : stageAfter.entrySet()) {
            deps.put(e.getKey(), e.getValue() == null ? new ArrayList<>() : new ArrayList<>(e.getValue()));
        }
        for (Map.Entry<String, List<String>> e : deps.entrySet()) {
            for (String dep : e.getValue()) {
                if (!deps.containsKey(dep)) {
                    throw new IllegalStateException(
                            "阶段 '" + e.getKey() + "' 依赖了不存在的阶段 '" + dep + "'");
                }
            }
        }
        List<String> order = topologicalSort(deps);
        return new PipelineTopology(deps, order);
    }

    private static List<String> topologicalSort(Map<String, List<String>> deps) {
        List<String> order = new ArrayList<>();
        Set<String> visited = new LinkedHashSet<>();
        Set<String> onPath = new LinkedHashSet<>();
        for (String stage : deps.keySet()) {
            visit(stage, deps, visited, onPath, order);
        }
        return Collections.unmodifiableList(order);
    }

    private static void visit(String stage, Map<String, List<String>> deps,
                              Set<String> visited, Set<String> onPath, List<String> order) {
        if (visited.contains(stage)) {
            return;
        }
        if (!onPath.add(stage)) {
            throw new IllegalStateException("流水线存在循环依赖，涉及阶段 '" + stage + "'");
        }
        for (String dep : deps.get(stage)) {
            visit(dep, deps, visited, onPath, order);
        }
        onPath.remove(stage);
        visited.add(stage);
        order.add(stage);
    }

    /** @return 拓扑顺序（上游阶段在前）。 */
    public List<String> order() {
        return order;
    }

    /** @return 指定阶段的直接依赖（after）列表。 */
    public List<String> dependenciesOf(String stage) {
        return dependencies.getOrDefault(stage, Collections.emptyList());
    }

    /** @return 叶子阶段（不被任何其它阶段依赖），按拓扑顺序。 */
    public List<String> leaves() {
        Set<String> referenced = new HashSet<>();
        for (List<String> deps : dependencies.values()) {
            referenced.addAll(deps);
        }
        List<String> leaves = new ArrayList<>();
        for (String stage : order) {
            if (!referenced.contains(stage)) {
                leaves.add(stage);
            }
        }
        return leaves;
    }
}
