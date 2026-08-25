package com.sstlfsj.disruptor.example.pay;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
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
    private static final EventTranslatorOneArg<PayEvent, String> TRANSLATOR =
            (event, sequence, payId) -> {
                event.setPayId(payId);
                event.setPersisted(false);
                event.setAudited(false);
            };

    private final DisruptorRuntime runtime;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo2 PipelineSpec + 原生 topology（pay）====");
        PayService.latch = new CountDownLatch(4);
        runtime.require("pay", PayEvent.class).ringBuffer().publishEvent(TRANSLATOR, "P-1");
        if (!PayService.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo2 超时");
        }
        results.markDone("pay");
        log.info("==== demo2 完成 ====");
    }
}
