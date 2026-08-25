package com.sstlfsj.disruptor.example.pay;

import com.sstlfsj.disruptor.core.PipelineSpec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PayPipelineConfig {

    @Bean
    public PipelineSpec<PayEvent> payPipeline(PayService svc) {
        return PipelineSpec.builder("pay", PayEvent.class, PayEvent::new)
                .topology(disruptor -> disruptor
                        .handleEventsWith((event, sequence, endOfBatch) -> svc.validate(event))
                        .then(
                                (event, sequence, endOfBatch) -> svc.persist(event),
                                (event, sequence, endOfBatch) -> svc.audit(event))
                        .then((event, sequence, endOfBatch) -> svc.notify(event)))
                .build();
    }
}
