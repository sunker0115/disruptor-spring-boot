package com.sstlfsj.disruptor.tutorial.match;

import java.math.BigDecimal;

/**
 * 不可变撮合产出，包含成交、挂单和完成三种形态。每条结果带确定性 {@code sequence}
 * （由 {@link OrderBook#nextSequence()} 赋予），同样的输入顺序会得到同样的结果顺序，可用于回放验证。
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
