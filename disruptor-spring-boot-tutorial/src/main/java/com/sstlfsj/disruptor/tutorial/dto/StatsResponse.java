package com.sstlfsj.disruptor.tutorial.dto;

import java.math.BigDecimal;

/** 成交统计。 */
public record StatsResponse(long tradeCount, BigDecimal tradedVolume) {}
