package com.sstlfsj.disruptor.tutorial.web;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.tutorial.pipeline.OrderEvent;
import com.sstlfsj.disruptor.tutorial.sink.MatchMetrics;
import com.sstlfsj.disruptor.tutorial.dto.AcceptedResponse;
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

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 下单入口（MQ 消费者的薄替身）：DTO → {@code tryPublish} 进环 → 202 受理回执 / 429 背压。
 * <b>生产环境把本类换成 MQ 监听器，调用同一个 {@code tryPublish}，body 不变。</b>
 * 撮合异步——不同步返回成交，结果从 {@code GET /orders/stats} 与 {@code GET /book} 观测。
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final EventBus eventBus;
    private final MatchMetrics metrics;
    private final AtomicLong idGen = new AtomicLong();

    @PostMapping
    public ResponseEntity<?> place(@RequestBody PlaceOrderRequest req) {
        if (req.symbol() == null || req.symbol().isBlank()
                || req.side() == null
                || req.price() == null || req.price().signum() <= 0
                || req.quantity() == null || req.quantity().signum() <= 0) {
            return ResponseEntity.badRequest().body("symbol/side 必填，price/quantity 必须为正");
        }

        long id = idGen.incrementAndGet();
        boolean accepted = eventBus.tryPublish(OrderEvent.class, e -> {
            e.setOrderId(id);
            e.setSymbol(req.symbol());
            e.setSide(req.side());
            e.setPrice(req.price());
            e.setQuantity(req.quantity());
            e.setTransactTime(System.currentTimeMillis());
        });

        if (accepted) {
            log.info("[matching/accept] 受理订单 {} symbol={} side={} price={} qty={}",
                    id, req.symbol(), req.side(), req.price(), req.quantity());
            return ResponseEntity.accepted().body(new AcceptedResponse(id));
        }
        log.warn("[matching/reject] 背压拒绝 symbol={} side={} remaining={}",
                req.symbol(), req.side(), eventBus.remainingCapacity(OrderEvent.class));
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("撮合繁忙，请重试");
    }

    @GetMapping("/stats")
    public StatsResponse stats() {
        return new StatsResponse(metrics.tradeCount(), metrics.tradedVolume());
    }
}
