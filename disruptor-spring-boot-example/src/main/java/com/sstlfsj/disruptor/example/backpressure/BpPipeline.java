package com.sstlfsj.disruptor.example.backpressure;

import com.sstlfsj.disruptor.core.PipelineSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/** demo5：慢消费阶段，让 ring buffer 快速被填满以触发 tryPublish 背压。 */
@Component
public class BpPipeline {

    private static final Logger log = LoggerFactory.getLogger(BpPipeline.class);

    @Bean
    public PipelineSpec<BpEvent> backpressurePipelineSpec() {
        return PipelineSpec.builder("backpressure", BpEvent.class, BpEvent::new)
                .bufferSize(16)
                .topology(disruptor -> disruptor.handleEventsWith(
                        (event, sequence, endOfBatch) -> slow(event)))
                .build();
    }

    public void slow(BpEvent e) throws InterruptedException {
        Thread.sleep(20);   // 模拟慢业务
        log.info("[backpressure/slow] 处理 n={}", e.getN());
    }
}
