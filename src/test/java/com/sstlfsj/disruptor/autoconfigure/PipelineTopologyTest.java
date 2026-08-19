package com.sstlfsj.disruptor.autoconfigure;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PipelineTopology 纯逻辑单元测试：默认阶段、线性/菱形拓扑排序、缺失依赖与环校验。
 */
class PipelineTopologyTest {

    @Test
    void emptyConfigYieldsSingleDefaultStage() {
        PipelineTopology topo = PipelineTopology.build(Map.of());
        assertTrue(topo.isSingleStage(), "空配置应只有 default 单阶段");
        assertEquals(List.of("default"), topo.order());
    }

    @Test
    void linearPipelineIsOrderedUpstreamFirst() {
        PipelineTopology topo = PipelineTopology.build(Map.of(
                "b", List.of("a"),
                "c", List.of("b"),
                "a", List.of()));
        List<String> order = topo.order();
        assertTrue(order.indexOf("a") < order.indexOf("b"), "a 应在 b 之前");
        assertTrue(order.indexOf("b") < order.indexOf("c"), "b 应在 c 之前");
        assertTrue(order.contains("default"), "default 阶段应始终存在");
    }

    @Test
    void diamondPipelineOrdersJoinAfterBothBranches() {
        PipelineTopology topo = PipelineTopology.build(Map.of(
                "a", List.of(),
                "b", List.of("a"),
                "c", List.of("a"),
                "d", List.of("b", "c")));
        List<String> order = topo.order();
        assertTrue(order.indexOf("a") < order.indexOf("b"));
        assertTrue(order.indexOf("a") < order.indexOf("c"));
        assertTrue(order.indexOf("b") < order.indexOf("d"), "d 应在 b 之后");
        assertTrue(order.indexOf("c") < order.indexOf("d"), "d 应在 c 之后");
    }

    @Test
    void missingDependencyFailsFast() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> PipelineTopology.build(Map.of("b", List.of("nonexistent"))));
        assertTrue(ex.getMessage().contains("nonexistent"));
    }

    @Test
    void cyclicDependencyFailsFast() {
        assertThrows(IllegalStateException.class,
                () -> PipelineTopology.build(Map.of(
                        "a", List.of("b"),
                        "b", List.of("a"))));
    }
}
