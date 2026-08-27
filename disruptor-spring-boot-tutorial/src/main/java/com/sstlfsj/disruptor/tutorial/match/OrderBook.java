package com.sstlfsj.disruptor.tutorial.match;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * 单 symbol 盘口：bids/asks 两侧 + 价格 long 编码 + 输出 sequence。
 * <b>故意非线程安全</b>（裸集合 + {@code ++sequence}），要求单线程调用者——由 Disruptor 单消费者保证，
 * 通过线程约束保证无锁状态更新的正确性。
 */
public final class OrderBook {

    private static final int PRICE_PRECISION = 8;
    private static final BigDecimal PRICE_SCALE = BigDecimal.TEN.pow(PRICE_PRECISION);

    private final String symbol;
    private final OrderSide bids = new OrderSide(true);
    private final OrderSide asks = new OrderSide(false);
    private long sequence;

    public OrderBook(String symbol) {
        this.symbol = symbol;
    }

    /** 价格 → long 编码（= price × 10^8，FLOOR 取整），使档位可用 long 键有序存储。 */
    public long toPriceLong(BigDecimal price) {
        return price.multiply(PRICE_SCALE).setScale(0, RoundingMode.FLOOR).longValueExact();
    }

    /** 本侧盘口（BUY→bids）。 */
    public OrderSide side(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    /** 对手盘（BUY 的对手 = asks）。 */
    public OrderSide counter(Side side) {
        return side == Side.BUY ? asks : bids;
    }

    public long nextSequence() { return ++sequence; }

    public String symbol() { return symbol; }

    /** 盘口档位（价 + 剩余量）。 */
    public record Level(BigDecimal price, BigDecimal quantity) {}

    /** 取某侧前 levels 档（buy 价降序 / sell 价升序）。 */
    public List<Level> depth(Side side, int levels) {
        List<Level> out = new ArrayList<>();
        side(side).forEach(bucket -> {
            if (out.size() >= levels) {
                return false;
            }
            out.add(new Level(bucket.price(), bucket.quantity()));
            return true;
        });
        return out;
    }
}
