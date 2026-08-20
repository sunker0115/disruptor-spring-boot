package com.sstlfsj.disruptor.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PipelineTopology 纯逻辑单元测试：线性/菱形拓扑排序、叶子识别、缺失依赖与环校验。
 */
class PipelineTopologyTest {

    @Test
    void linearPipelineOrderedUpstreamFirstSingleLeaf() {
        PipelineTopology topo = PipelineTopology.build(Map.of(
                "a", List.of(),
                "b", List.of("a"),
                "c", List.of("b")));
        List<String> order = topo.order();
        assertTrue(order.indexOf("a") < order.indexOf("b"), "a 应在 b 之前");
        assertTrue(order.indexOf("b") < order.indexOf("c"), "b 应在 c 之前");
        assertEquals(List.of("c"), topo.leaves(), "c 是唯一叶子");
    }

    @Test
    void diamondPipelineOrdersAndSingleLeaf() {
        PipelineTopology topo = PipelineTopology.build(Map.of(
                "a", List.of(),
                "b", List.of("a"),
                "c", List.of("a"),
                "d", List.of("b", "c")));
        List<String> order = topo.order();
        assertTrue(order.indexOf("a") < order.indexOf("b"));
        assertTrue(order.indexOf("a") < order.indexOf("c"));
        assertTrue(order.indexOf("b") < order.indexOf("d"), "d 在 b 之后");
        assertTrue(order.indexOf("c") < order.indexOf("d"), "d 在 c 之后");
        assertEquals(List.of("d"), topo.leaves(), "d 是唯一叶子");
    }

    @Test
    void multipleLeavesIdentified() {
        PipelineTopology topo = PipelineTopology.build(Map.of(
                "a", List.of(),
                "b", List.of("a"),
                "c", List.of("a")));
        List<String> leaves = topo.leaves();
        assertTrue(leaves.contains("b") && leaves.contains("c"), "b、c 都是叶子");
        assertFalse(leaves.contains("a"), "a 不是叶子");
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
