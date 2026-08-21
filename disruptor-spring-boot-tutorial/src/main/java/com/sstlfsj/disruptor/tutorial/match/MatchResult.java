package com.sstlfsj.disruptor.tutorial.match;

import java.math.BigDecimal;

/**
 * 不可变撮合产出，三形态。每条带确定性 {@code sequence}（{@link OrderBook#nextSequence()} 赋予，
 * 同输入同序 → 同 sequence，可回放）。精简自 raftkit {@code match.core.MatchResult}：LIMIT 只有 FILLED
 * 一种完成，故去掉 DoneReason / userId / execId。
 */
public sealed interface MatchResult permits MatchResult.Trade, MatchResult.Open, MatchResult.Done {

    String symbol();

    long sequence();

    /** 一笔成交（taker 吃掉 maker 的 quantity）。 */
    record Trade(String symbol, long sequence,
                 long takerOrderId, long makerOrderId,
                 Side takerSide, BigDecimal price, BigDecimal quantity,
                 long tradeTime) implements MatchResult {}

    /** 订单进入盘口挂单。 */
    record Open(String symbol, long sequence,
                long orderId, Side side, BigDecimal price, BigDecimal remaining) implements MatchResult {}

    /** 订单完成（LIMIT 场景即全部成交）。 */
    record Done(String symbol, long sequence,
                long orderId, Side side, BigDecimal remaining, boolean taker) implements MatchResult {}
}
