package com.sstlfsj.disruptor.example.order;

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
@Order(1)
@RequiredArgsConstructor
public class OrderDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(OrderDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo1 声明式菱形 DAG（order）====");
        OrderPipeline.latch = new CountDownLatch(4);
        eventBus.publish(OrderEvent.class, e -> {
            e.setOrderId("A-1");
            e.setAmount(199);
        });
        if (!OrderPipeline.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo1 超时");
        }
        results.markDone("order");
        log.info("==== demo1 完成 ====");
    }
}
