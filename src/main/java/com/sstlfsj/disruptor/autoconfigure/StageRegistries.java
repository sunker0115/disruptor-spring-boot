package com.sstlfsj.disruptor.autoconfigure;

import com.sstlfsj.disruptor.event.ConsumerRegistry;
import com.sstlfsj.disruptor.event.DefaultConsumerRegistry;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 持有每个处理阶段独立的 {@link ConsumerRegistry}。{@code default} 阶段复用传入的
 * 公开 ConsumerRegistry bean（命令式 subscribe 作用于 default 阶段），其余阶段各自新建。
 */
public class StageRegistries {

    private final Map<String, ConsumerRegistry> registries = new LinkedHashMap<>();

    public StageRegistries(PipelineTopology topology, ConsumerRegistry defaultRegistry) {
        for (String stage : topology.stages()) {
            registries.put(stage,
                    PipelineTopology.DEFAULT_STAGE.equals(stage)
                            ? defaultRegistry
                            : new DefaultConsumerRegistry());
        }
    }

    /** @return 指定阶段的 registry；阶段不存在返回 {@code null}。 */
    public ConsumerRegistry forStage(String stage) {
        return registries.get(stage);
    }

    /** @return 是否存在指定阶段。 */
    public boolean hasStage(String stage) {
        return registries.containsKey(stage);
    }
}
