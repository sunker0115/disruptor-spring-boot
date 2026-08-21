package com.sstlfsj.disruptor.example.nospring;

import com.sstlfsj.disruptor.core.DefaultEventBus;
import com.sstlfsj.disruptor.core.DisruptorConfig;
import com.sstlfsj.disruptor.core.DisruptorPipeline;
import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.core.EventPipeline;
import com.sstlfsj.disruptor.core.PipelineBuilder;
import com.sstlfsj.disruptor.core.Pipelines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 无 Spring 的纯 Java 用法：手工装配 PipelineBuilder + DefaultEventBus，证明 core 可独立使用。
 * 运行方式：直接运行本类 main。
 */
public class PureJavaExample {

    private static final Logger log = LoggerFactory.getLogger(PureJavaExample.class);

    public static class PlainEvent {
        int n;
    }

    public static void main(String[] args) throws Exception {
        CountDownLatch latch = new CountDownLatch(3);

        DisruptorConfig config = new DisruptorConfig(
                16, DisruptorConfig.WaitStrategyType.BLOCKING, Duration.ofSeconds(5));

        EventPipeline<PlainEvent> def = EventPipeline.builder("plain", PlainEvent.class)
                .stage("handle", e -> {
                    log.info("[pure-java] 处理 n={}", e.n);
                    latch.countDown();
                })
                .build();

        DisruptorPipeline<PlainEvent> pipeline = new PipelineBuilder(config).build(def);
        Pipelines pipelines = new Pipelines();
        pipelines.register(pipeline);
        EventBus bus = new DefaultEventBus(pipelines);

        pipeline.disruptor().start();
        try {
            for (int i = 0; i < 3; i++) {
                int n = i;
                bus.publish(PlainEvent.class, e -> e.n = n);
            }
            if (!latch.await(3, TimeUnit.SECONDS)) {
                log.warn("[pure-java] 超时");
            }
        } finally {
            pipeline.disruptor().shutdown(2, TimeUnit.SECONDS);
        }
        log.info("[pure-java] 完成");
    }
}
