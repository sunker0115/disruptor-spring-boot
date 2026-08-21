package com.sstlfsj.disruptor.tutorial.dto;

import com.sstlfsj.disruptor.tutorial.match.Side;

import java.math.BigDecimal;

/** 下单请求体。 */
public record PlaceOrderRequest(String symbol, Side side, BigDecimal price, BigDecimal quantity) {}
