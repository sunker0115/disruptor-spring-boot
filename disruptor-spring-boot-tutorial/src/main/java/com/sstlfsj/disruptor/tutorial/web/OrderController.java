package com.sstlfsj.disruptor.tutorial.web;

import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineHandle;
import com.sstlfsj.disruptor.tutorial.pipeline.OrderEvent;
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

    private final DisruptorRuntime runtime;
    private final MatchMetrics metrics;
    private final BatchPersistSink batchPersistSink;
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
        PipelineHandle<OrderEvent> matching = runtime.require("matching", OrderEvent.class);

        // 发布进环（非阻塞，ring 满返回 false → 走下面的 429 背压）。此处用门面 tryPublish(Consumer)：
        // 日常推荐写法——可读、只依赖 PipelineHandle、不暴露 RingBuffer；代价是每次捕获 id/req 产生一次
        // lambda 分配（业务吞吐可忽略）。
        boolean accepted = matching.tryPublish(event -> {
            event.setOrderId(id);
            event.setSymbol(req.symbol());
            event.setSide(req.side());
            event.setPrice(req.price());
            event.setQuantity(req.quantity());
            event.setTransactTime(System.currentTimeMillis());
        });
        // —— 对照：零分配写法（纳秒级热路径 / GC 敏感时选它）。静态 EventTranslator 单例 + 参数透传 →
        //    零捕获零分配；代价是要维护一个静态字段并直达 ringBuffer()。切到这种写法需
        //    import com.lmax.disruptor.EventTranslatorTwoArg，并加类字段：
        //    private static final EventTranslatorTwoArg<OrderEvent, Long, PlaceOrderRequest> TRANSLATOR =
        //            (event, sequence, orderId, request) -> { event.setOrderId(orderId); /* ...其余 setter... */ };
        //    然后：boolean accepted = matching.ringBuffer().tryPublishEvent(TRANSLATOR, id, req);

        if (accepted) {
            log.info("[matching/accept] 受理订单 {} symbol={} side={} price={} qty={}",
                    id, req.symbol(), req.side(), req.price(), req.quantity());
            return ResponseEntity.accepted().body(new AcceptedResponse(id));
        }
        log.warn("[matching/reject] 背压拒绝 symbol={} side={} remaining={}",
                req.symbol(), req.side(), matching.remaining());
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
