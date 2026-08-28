package com.sstlfsj.disruptor.tutorial.web;

import com.sstlfsj.disruptor.tutorial.ingress.MatchingOrderIngress;
import com.sstlfsj.disruptor.tutorial.sink.BatchPersistSink;
import com.sstlfsj.disruptor.tutorial.sink.MatchMetrics;
import com.sstlfsj.disruptor.tutorial.dto.AcceptedResponse;
import com.sstlfsj.disruptor.tutorial.dto.PersistStatsResponse;
import com.sstlfsj.disruptor.tutorial.dto.PlaceOrderRequest;
import com.sstlfsj.disruptor.tutorial.dto.StatsResponse;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.OptionalLong;

/**
 * 下单入口（MQ 消费者的薄替身）：并发 HTTP 请求先交给单一订单入口线程，
 * 再由该线程原生发布到 SINGLE RingBuffer，真实入环后返回 202，否则返回 429。
 * 撮合异步——不同步返回成交，结果从 {@code GET /orders/stats} 与 {@code GET /book} 观测。
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final MatchingOrderIngress orderIngress;
    private final MatchMetrics metrics;
    private final BatchPersistSink batchPersistSink;

    @PostMapping
    public ResponseEntity<?> place(@RequestBody PlaceOrderRequest req) {
        if (req.symbol() == null || req.symbol().isBlank()
                || req.side() == null
                || req.price() == null || req.price().signum() <= 0
                || req.quantity() == null || req.quantity().signum() <= 0) {
            return ResponseEntity.badRequest().body("symbol/side 必填，price/quantity 必须为正");
        }

        OptionalLong publishedOrderId = orderIngress.tryPublish(req);
        if (publishedOrderId.isPresent()) {
            long id = publishedOrderId.getAsLong();
            log.info("[matching/accept] 受理订单 {} symbol={} side={} price={} qty={}",
                    id, req.symbol(), req.side(), req.price(), req.quantity());
            return ResponseEntity.accepted().body(new AcceptedResponse(id));
        }
        log.warn("[matching/reject] 订单未入环 symbol={} side={}", req.symbol(), req.side());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("撮合繁忙，请重试");
    }

    @GetMapping("/stats")
    public StatsResponse stats() {
        return new StatsResponse(metrics.tradeCount(), metrics.tradedVolume());
    }

    @GetMapping("/persist-stats")
    public PersistStatsResponse persistStats() {
        return new PersistStatsResponse(batchPersistSink.flushCount(), batchPersistSink.persistedCount(),
                batchPersistSink.lastBatchSize(), batchPersistSink.maxBatchSize());
    }
}
