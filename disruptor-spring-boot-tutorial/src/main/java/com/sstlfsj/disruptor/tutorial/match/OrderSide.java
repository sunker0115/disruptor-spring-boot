package com.sstlfsj.disruptor.tutorial.match;

import java.math.BigDecimal;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * 单侧盘口（bids 或 asks），价格档位存储直接内嵌 {@link TreeMap}（O(log n) 有序）。
 * 语义等价 raftkit {@code TreeMapPriceLevels}：best/forEach 买侧价格降序、卖侧升序。
 * 精简自 raftkit {@code match.core.OrderSide}：去 PriceLevels 接口层（只一种实现无需抽象）、去改单 reduce。
 */
public final class OrderSide {

    private final boolean buy;
    private final TreeMap<Long, PriceBucket> levels = new TreeMap<>();
    private BigDecimal quantity = BigDecimal.ZERO;

    public OrderSide(boolean buy) {
        this.buy = buy;
    }

    public void append(Order order) {
        PriceBucket bucket = levels.computeIfAbsent(order.getPriceLong(),
                k -> new PriceBucket(order.getPriceLong(), order.getPrice()));
        bucket.add(order);
        quantity = quantity.add(order.remaining());
    }

    public void remove(Order order) {
        PriceBucket bucket = levels.get(order.getPriceLong());
        if (bucket == null) {
            return;
        }
        Order removed = bucket.remove(order.getOrderId());
        if (removed == null) {
            return;
        }
        quantity = quantity.subtract(removed.remaining());
        if (bucket.isEmpty()) {
            levels.remove(bucket.priceLong());
        }
    }

    /** 成交后维护本侧总量（调用方已推进 order/bucket）。 */
    public void onFill(BigDecimal filledQty) {
        quantity = quantity.subtract(filledQty);
    }

    /** 最优档：buy=最高价、sell=最低价；空返回 null。 */
    public PriceBucket best() {
        var e = buy ? levels.lastEntry() : levels.firstEntry();
        return e == null ? null : e.getValue();
    }

    /** 有序遍历：buy=价格降序、sell=价格升序；visitor 返回 false 即停。 */
    public void forEach(Predicate<PriceBucket> visitor) {
        var values = (buy ? levels.descendingMap() : levels).values();
        for (PriceBucket b : values) {
            if (!visitor.test(b)) {
                break;
            }
        }
    }

    /** 对手 taker 价（long 编码）是否与本侧最优档交叉。 */
    public boolean crosses(long counterPriceLong) {
        PriceBucket best = best();
        if (best == null) {
            return false;
        }
        return buy ? counterPriceLong <= best.priceLong() : counterPriceLong >= best.priceLong();
    }

    public boolean isEmpty() { return levels.isEmpty(); }
    public BigDecimal quantity() { return quantity; }
}
