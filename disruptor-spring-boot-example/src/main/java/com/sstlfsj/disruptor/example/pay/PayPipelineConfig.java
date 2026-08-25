package com.sstlfsj.disruptor.example.pay;

import com.sstlfsj.disruptor.core.PipelineSpec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PayPipelineConfig {

    /** demo2：通过 PipelineSpec.exceptionHandler(...) 注册自定义管道级异常策略。 */
    @Bean
    public PipelineSpec<PayEvent> payPipeline(PayService svc, PayExceptionHandler exceptionHandler) {
        return PipelineSpec.builder("pay", PayEvent.class, PayEvent::new)
                .exceptionHandler(exceptionHandler)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> svc.charge(event)))
                .build();
    }
}
