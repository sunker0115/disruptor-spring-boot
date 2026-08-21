package com.sstlfsj.disruptor.example.reuse;

import com.sstlfsj.disruptor.core.EventBus;
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
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo4 事件复用 Resettable（reuse）====");
        int followers = 20;                 // > buffer-size(16)，确保槽位被复用
        ReusePipeline.seen.clear();
        ReusePipeline.latch = new CountDownLatch(1 + followers);
        eventBus.publish(ReuseEvent.class, e -> {          // 订单 A 用券
            e.setOrderId("A");
            e.setCouponCode("SAVE10");
        });
        for (int i = 0; i < followers; i++) {              // 后续订单只设 orderId
            String id = "B" + i;
            eventBus.publish(ReuseEvent.class, e -> e.setOrderId(id));
        }
        if (!ReusePipeline.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo4 超时");
        }
        boolean leaked = ReusePipeline.seen.stream()
                .anyMatch(s -> s.startsWith("B") && s.endsWith("SAVE10"));
        log.info("[reuse] 后续订单是否读到残留 SAVE10：{}（reset 生效应为 false）", leaked);
        results.markDone("reuse");
        log.info("==== demo4 完成 ====");
    }
}
