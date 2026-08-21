package com.sstlfsj.disruptor.example.pay;

import com.sstlfsj.disruptor.core.EventPipeline;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PayPipelineConfig {

    @Bean
    public EventPipeline<PayEvent> payPipeline(PayService svc) {
        return EventPipeline.builder("pay", PayEvent.class)
                .stage("validate", svc::validate)
                .stage("persist", svc::persist).after("validate")
                .stage("audit", svc::audit).after("validate")
                .stage("notify", svc::notify).after("persist", "audit")
                .build();
    }
}
