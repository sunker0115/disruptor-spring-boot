package com.sstlfsj.disruptor.tutorial.dto;

import com.sstlfsj.disruptor.tutorial.match.OrderBook;

import java.util.List;

/** 盘口深度（买卖两侧档位，价 + 剩余量）。 */
public record BookResponse(String symbol, List<OrderBook.Level> bids, List<OrderBook.Level> asks) {}
