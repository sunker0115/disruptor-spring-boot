package com.sstlfsj.disruptor.tutorial.pipeline;

import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.core.PipelineSpec;
import com.sstlfsj.disruptor.tutorial.match.MatchEngine;
import com.sstlfsj.disruptor.tutorial.match.MatchResult;
import com.sstlfsj.disruptor.tutorial.match.Order;
import com.sstlfsj.disruptor.tutorial.sink.BatchPersistSink;
import com.sstlfsj.disruptor.tutorial.sink.MatchMetrics;
import com.sstlfsj.disruptor.tutorial.sink.MatchResultSink;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 撮合管道（pipeline = "matching"）DAG：{@code match}（源头）→ fan-out 到 {@code emit ‖ metrics ‖ persist}。
 *
 * <p><b>match 必须单线程</b>：{@link MatchEngine}/盘口非线程安全，靠单消费者线程无锁串行撮合
 * 保证正确——这是本 tutorial 的立论核心（对比线程池：要么加锁争用、要么并发算错）。</p>
 *
 * <p>{@code persist} 是唯一用到 {@code endOfBatch} 的 stage：逐条攒、批尾一次性落库，演示 Disruptor
 * 自适应批处理（闲时批≈1 低延迟、忙时批自动变大摊薄 IO）。见 {@link BatchPersistSink}。</p>
 */
@Component
@RequiredArgsConstructor
public class MatchingPipeline {

    private static final Logger log = LoggerFactory.getLogger(MatchingPipeline.class);

    private final MatchEngine engine;
    private final MatchResultSink sink;
    private final MatchMetrics metrics;
    private final BatchPersistSink batchPersistSink;

    @Bean
    public PipelineSpec<OrderEvent> matchingPipelineSpec() {
        return PipelineSpec.builder("matching", OrderEvent.class, OrderEvent::new)
                .producerType(ProducerType.SINGLE)
                .topology(disruptor -> disruptor
                        .handleEventsWith((event, sequence, endOfBatch) -> match(event))
                        .then(
                                (event, sequence, endOfBatch) -> emit(event),
                                (event, sequence, endOfBatch) -> metrics(event),
                                (event, sequence, endOfBatch) -> persist(event, endOfBatch)))
                .build();
    }

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

    public void emit(OrderEvent e) {
        sink.accept(e.getResults());
    }

    public void metrics(OrderEvent e) {
        metrics.accumulate(e.getResults());
    }

    /** 逐条攒本条产出，直到 {@code endOfBatch=true} 才一次性落库——演示 Disruptor 自适应批处理。 */
    public void persist(OrderEvent e, boolean endOfBatch) {
        batchPersistSink.stage(e.getResults());
        if (endOfBatch) {
            batchPersistSink.flush();
        }
    }
}
