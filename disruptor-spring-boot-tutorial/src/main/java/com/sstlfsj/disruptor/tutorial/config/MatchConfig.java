package com.sstlfsj.disruptor.tutorial.config;

import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.tutorial.match.MatchEngine;
import com.sstlfsj.disruptor.tutorial.pipeline.MatchingPipeline;
import com.sstlfsj.disruptor.tutorial.pipeline.OrderEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 把撮合核心暴露为 bean（核心本身保持零 Spring 注解，可脱离容器独立使用）。
 */
@Configuration
public class MatchConfig {

    @Bean
    public MatchEngine matchEngine() {
        return new MatchEngine();
    }

    /**
     * HTTP 请求先进入有界单线程任务队列；唯一 worker 再发布到 SINGLE 撮合管道。
     * Spring 的并发自动配置负责启动和优雅停止这个显式根对象。
     */
    @Bean(destroyMethod = "")
    public EventLoop matchingOrderIngressEventLoop(DisruptorRuntime runtime) {
        int capacity = runtime.require(MatchingPipeline.PIPELINE_NAME, OrderEvent.class)
                .unsafeRingBuffer()
                .getBufferSize();
        return EventLoopBuilder.bounded("matching-order-ingress", capacity).build();
    }
}
