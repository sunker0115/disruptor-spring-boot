package com.sstlfsj.disruptor.tutorial.match;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * 撮合订单（可变：累计 {@code executedQty}）。仅撮合线程访问，非线程安全——由 Disruptor 单消费者保证。
 * 教程只保留 LIMIT 订单撮合所需字段。
 */
@Getter
@Setter
@Builder
public class Order {

    private long orderId;
    private String symbol;
    private Side side;
    private BigDecimal price;
    private BigDecimal quantity;

    @Builder.Default
    private BigDecimal executedQty = BigDecimal.ZERO;

    /** 价格的 long 编码（price × 10^8），由 OrderBook 入盘前计算。 */
    private long priceLong;
    private long transactTime;

    /** 未成交量。 */
    public BigDecimal remaining() {
        return quantity.subtract(executedQty);
    }
}
