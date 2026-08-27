package com.sstlfsj.disruptor.tutorial.match;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 同一价位的订单桶：{@link LinkedHashMap} 保插入序 → 价格时间优先（同价先到先撮）。
 * {@code quantity} 跟踪本桶剩余总量，增删或成交时增量维护。
 */
public final class PriceBucket {

    private final long priceLong;
    private final BigDecimal price;
    private final LinkedHashMap<Long, Order> orders = new LinkedHashMap<>();
    private BigDecimal quantity = BigDecimal.ZERO;

    public PriceBucket(long priceLong, BigDecimal price) {
        this.priceLong = priceLong;
        this.price = price;
    }

    public void add(Order order) {
        orders.put(order.getOrderId(), order);
        quantity = quantity.add(order.remaining());
    }

    /** 移除并扣减 quantity（按订单当前 remaining）。 */
    public Order remove(long orderId) {
        Order o = orders.remove(orderId);
        if (o != null) {
            quantity = quantity.subtract(o.remaining());
        }
        return o;
    }

    /** 成交 filledQty：桶剩余量减少（调用方已推进 order.executedQty）。 */
    public void onFill(BigDecimal filledQty) {
        quantity = quantity.subtract(filledQty);
    }

    public long priceLong() { return priceLong; }
    public BigDecimal price() { return price; }
    public BigDecimal quantity() { return quantity; }
    public Map<Long, Order> orders() { return orders; }
    public boolean isEmpty() { return orders.isEmpty(); }
}
