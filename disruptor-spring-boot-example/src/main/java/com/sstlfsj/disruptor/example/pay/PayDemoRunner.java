package com.sstlfsj.disruptor.example.pay;

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
@Order(2)
@RequiredArgsConstructor
public class PayDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PayDemoRunner.class);
    private static final EventTranslatorTwoArg<PayEvent, String, Long> TRANSLATOR =
            (event, sequence, payId, amount) -> {
                event.setPayId(payId);
                event.setAmount(amount);
            };

    private final DisruptorRuntime runtime;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo2 自定义 ExceptionHandler（pay）====");
        // 3 个事件：正常、非法（抛异常）、正常。若三者都 countDown，说明异常被自定义
        // handler 接住，且异常之后的事件仍被处理——管道未被单次失败中断。
        PayService.latch = new CountDownLatch(3);
        PipelineHandle<PayEvent> handle = runtime.require("pay", PayEvent.class);
        handle.publishEvent(TRANSLATOR, "P-1", 100L);
        handle.publishEvent(TRANSLATOR, "P-BAD", -1L);
        handle.publishEvent(TRANSLATOR, "P-2", 200L);
        if (!PayService.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo2 超时");
        }
        results.markDone("pay");
        log.info("==== demo2 完成 ====");
    }
}
