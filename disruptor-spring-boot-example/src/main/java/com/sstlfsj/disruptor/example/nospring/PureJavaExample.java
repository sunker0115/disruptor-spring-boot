package com.sstlfsj.disruptor.example.nospring;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineSettings;
import com.sstlfsj.disruptor.core.PipelineSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 无 Spring 的纯 Java 用法：直接构建 PipelineSpec 与 DisruptorRuntime。
 * 运行方式：直接运行本类 main。
 */
public class PureJavaExample {

    private static final Logger log = LoggerFactory.getLogger(PureJavaExample.class);
    private static final EventTranslatorOneArg<PlainEvent, Integer> TRANSLATOR =
            (event, sequence, value) -> event.n = value;

    public static class PlainEvent {
        int n;
    }

    public static void main(String[] args) throws Exception {
        CountDownLatch latch = new CountDownLatch(3);

        PipelineSpec<PlainEvent> spec = PipelineSpec.builder("plain", PlainEvent.class, PlainEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                    log.info("[pure-java] 处理 n={}", event.n);
                    latch.countDown();
                }))
                .build();
        PipelineSettings settings = PipelineSettings.builder()
                .bufferSize(16)
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder()
                .settings(settings)
                .shutdownTimeout(Duration.ofSeconds(5))
                .add(spec)
                .build();

        runtime.start();
        try {
            for (int i = 0; i < 3; i++) {
                runtime.require("plain", PlainEvent.class).publishEvent(TRANSLATOR, i);
            }
            if (!latch.await(3, TimeUnit.SECONDS)) {
                log.warn("[pure-java] 超时");
            }
        } finally {
            runtime.shutdown();
        }
        log.info("[pure-java] 完成");
    }
}
