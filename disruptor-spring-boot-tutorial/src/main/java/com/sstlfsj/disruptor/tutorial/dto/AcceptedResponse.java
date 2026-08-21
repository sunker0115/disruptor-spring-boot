package com.sstlfsj.disruptor.tutorial.dto;

/** 下单受理回执（撮合异步，仅返回 orderId，不含成交结果）。 */
public record AcceptedResponse(long orderId) {}
