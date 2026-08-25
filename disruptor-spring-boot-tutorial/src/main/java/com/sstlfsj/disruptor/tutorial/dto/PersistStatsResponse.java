package com.sstlfsj.disruptor.tutorial.dto;

/** 批量落库观测（演示 endOfBatch 自适应批处理）：flush 次数 / 累计落库条数 / 最近一批与见过的最大批。 */
public record PersistStatsResponse(long flushCount, long persistedCount, long lastBatchSize, long maxBatchSize) {}
