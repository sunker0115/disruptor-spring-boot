package com.sstlfsj.disruptor.tutorial.pipeline;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import com.sstlfsj.disruptor.tutorial.match.MatchEngine;
import com.sstlfsj.disruptor.tutorial.match.MatchResult;
import com.sstlfsj.disruptor.tutorial.match.Order;
import com.sstlfsj.disruptor.tutorial.sink.MatchMetrics;
import com.sstlfsj.disruptor.tutorial.sink.MatchResultSink;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 撮合管道（pipeline = "matching"）DAG：{@code match}（源头）→ fan-out 到 {@code emit ‖ metrics}。
 *
 * <p><b>match 必须 parallelism = 1</b>：{@link MatchEngine}/盘口非线程安全，靠单消费者线程无锁串行撮合
 * 保证正确——这是本 tutorial 的立论核心（对比线程池：要么加锁争用、要么并发算错）。</p>
 */
@Component
@RequiredArgsConstructor
public class MatchingPipeline {

    private static final Logger log = LoggerFactory.getLogger(MatchingPipeline.class);

    private final MatchEngine engine;
    private final MatchResultSink sink;
    private final MatchMetrics metrics;

    @DisruptorStage(pipeline = "matching", name = "match", parallelism = 1)
    public void match(OrderEvent e) {
        // 从复用槽位拷出独立 Order：ring 会被后续事件覆盖，而挂单要长期留在盘口，必须持有稳定副本。
        Order order = Order.builder()
                .orderId(e.getOrderId()).symbol(e.getSymbol()).side(e.getSide())
                .price(e.getPrice()).quantity(e.getQuantity()).transactTime(e.getTransactTime())
                .build();
        List<MatchResult> results = engine.handle(order);
        e.setResults(results);
        log.info("[matching/match] 订单 {} 撮合产出 {} 条结果", e.getOrderId(), e.getResults().size());
    }

    @DisruptorStage(pipeline = "matching", name = "emit", after = "match")
    public void emit(OrderEvent e) {
        sink.accept(e.getResults());
    }

    @DisruptorStage(pipeline = "matching", name = "metrics", after = "match")
    public void metrics(OrderEvent e) {
        metrics.accumulate(e.getResults());
    }
}
