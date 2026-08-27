package com.sstlfsj.disruptor.example.reuse;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineHandle;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
@Order(4)
@RequiredArgsConstructor
public class ReuseDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ReuseDemoRunner.class);
    private static final EventTranslatorTwoArg<ReuseEvent, String, String> WITH_COUPON =
            (event, sequence, orderId, couponCode) -> {
                event.setOrderId(orderId);
                event.setCouponCode(couponCode);
            };
    private static final EventTranslatorOneArg<ReuseEvent, String> WITHOUT_COUPON =
            (event, sequence, orderId) -> event.setOrderId(orderId);

    private final DisruptorRuntime runtime;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo4 原生清理 handler（reuse）====");
        int followers = 20;                 // > buffer-size(16)，确保槽位被复用
        ReusePipeline.seen.clear();
        ReusePipeline.latch = new CountDownLatch(1 + followers);
        PipelineHandle<ReuseEvent> pipeline = runtime.require("reuse", ReuseEvent.class);
        pipeline.publishEvent(WITH_COUPON, "A", "SAVE10");
        for (int i = 0; i < followers; i++) {              // 后续订单只设 orderId
            pipeline.publishEvent(WITHOUT_COUPON, "B" + i);
        }
        if (!ReusePipeline.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo4 超时");
        }
        boolean leaked = ReusePipeline.seen.stream()
                .anyMatch(s -> s.startsWith("B") && s.endsWith("SAVE10"));
        log.info("[reuse] 后续订单是否读到残留 SAVE10：{}（cleanup 生效应为 false）", leaked);
        results.markDone("reuse");
        log.info("==== demo4 完成 ====");
    }
}
