package com.sstlfsj.disruptor.example.concurrent;

import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoopGroup;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopGroupBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 显式声明由 Spring 生命周期聚合器管理的 concurrent 根对象。 */
@Configuration(proxyBeanMethods = false)
public class ConcurrentExampleConfiguration {

    @Bean(destroyMethod = "")
    public DisruptorEventLoop orderEventLoop() {
        return EventLoopBuilder.bounded("orders", 128).build();
    }

    @Bean(destroyMethod = "")
    public DisruptorEventLoopGroup orderWorkerGroup() {
        return EventLoopGroupBuilder.bounded("order-workers", 2, 64).build();
    }

    @Bean
    public OrderEventLoopService orderEventLoopService(
            DisruptorEventLoop orderEventLoop,
            DisruptorEventLoopGroup orderWorkerGroup) {
        return new OrderEventLoopService(orderEventLoop, orderWorkerGroup);
    }
}
