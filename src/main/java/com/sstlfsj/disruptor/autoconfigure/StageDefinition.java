package com.sstlfsj.disruptor.autoconfigure;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个处理阶段的定义，绑定自 {@code disruptor.pipeline.<stage>}。
 */
public class StageDefinition {

    /**
     * 本阶段依赖的上游阶段名。本阶段在这些上游阶段都处理完同一事件后才处理该事件。
     * 空 = 源头阶段（仅受发布进度门控）。
     */
    private List<String> after = new ArrayList<>();

    public List<String> getAfter() {
        return after;
    }

    public void setAfter(List<String> after) {
        this.after = after;
    }
}
