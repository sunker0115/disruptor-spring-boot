package com.sstlfsj.disruptor.tutorial.pipeline;

import com.sstlfsj.disruptor.tutorial.match.MatchResult;
import com.sstlfsj.disruptor.tutorial.match.Side;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.List;

/**
 * RingBuffer 复用槽位：承载一笔下单入参，撮合后由 {@code match} stage 把产出挂到 {@code results} 上，
 * 供下游 {@code emit}/{@code metrics} 读。可变、复用——同一实例会被后续事件覆盖。
 */
@Getter
@Setter
public class OrderEvent {

    private long orderId;
    private String symbol;
    private Side side;
    private BigDecimal price;
    private BigDecimal quantity;
    private long transactTime;

    /** 撮合产出（match 写、emit/metrics 读）。 */
    private List<MatchResult> results;
}
