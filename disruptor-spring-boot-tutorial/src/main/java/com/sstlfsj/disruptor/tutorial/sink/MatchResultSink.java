package com.sstlfsj.disruptor.tutorial.sink;

import com.sstlfsj.disruptor.tutorial.match.MatchResult;

import java.util.List;

/**
 * 撮合结果出口（薄接缝）。tutorial 用内存实现占位；<b>生产环境这里换成：发 MQ(match-results) → 清算/行情，
 * 或落库、推 websocket</b>。由 {@code emit} stage 单线程调用。
 */
public interface MatchResultSink {

    void accept(List<MatchResult> results);
}
