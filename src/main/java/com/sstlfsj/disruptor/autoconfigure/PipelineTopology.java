package com.sstlfsj.disruptor.autoconfigure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 流水线拓扑：由阶段依赖图构建，校验依赖存在性与无环，并给出确定的拓扑顺序
 * （上游在前）。纯逻辑，不依赖 Spring / Disruptor，便于单元测试。
 *
 * <p>隐式的 {@link #DEFAULT_STAGE} 阶段始终存在（默认无依赖的源头阶段），
 * 除非调用方在输入中显式声明它。</p>
 */
public final class PipelineTopology {

    /** 隐式默认阶段名；未标注 stage 的监听器与命令式 subscribe 均归入此阶段。 */
    public static final String DEFAULT_STAGE = "default";

    private final Map<String, List<String>> dependencies;
    private final List<String> order;

    private PipelineTopology(Map<String, List<String>> dependencies, List<String> order) {
        this.dependencies = dependencies;
        this.order = order;
    }

    /**
     * 构建并校验拓扑。
     *
     * @param stageAfter 各阶段声明的 after 依赖（不含隐式 default）；可为 null
     * @return 校验并拓扑排序后的拓扑（含隐式 default）
     * @throws IllegalStateException 依赖引用了不存在的阶段，或存在循环依赖
     */
    public static PipelineTopology build(Map<String, List<String>> stageAfter) {
        Map<String, List<String>> deps = new LinkedHashMap<>();
        deps.put(DEFAULT_STAGE, new ArrayList<>());
        if (stageAfter != null) {
            for (Map.Entry<String, List<String>> e : stageAfter.entrySet()) {
                deps.put(e.getKey(),
                        e.getValue() == null ? new ArrayList<>() : new ArrayList<>(e.getValue()));
            }
        }
        for (Map.Entry<String, List<String>> e : deps.entrySet()) {
            for (String dep : e.getValue()) {
                if (!deps.containsKey(dep)) {
                    throw new IllegalStateException(
                            "流水线阶段 '" + e.getKey() + "' 依赖了不存在的阶段 '" + dep + "'");
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

    /** @return 拓扑顺序（上游阶段在前），可安全用于依次注册 EventHandler。 */
    public List<String> order() {
        return order;
    }

    /** @return 指定阶段的直接依赖（after）列表；未知阶段返回空列表。 */
    public List<String> dependenciesOf(String stage) {
        return dependencies.getOrDefault(stage, Collections.emptyList());
    }

    /** @return 全部阶段名（含隐式 default）。 */
    public Set<String> stages() {
        return Collections.unmodifiableSet(dependencies.keySet());
    }

    /** @return 是否只有隐式 default 单一阶段（零流水线配置）。 */
    public boolean isSingleStage() {
        return dependencies.size() == 1;
    }
}
