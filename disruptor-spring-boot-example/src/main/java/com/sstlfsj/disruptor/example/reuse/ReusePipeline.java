package com.sstlfsj.disruptor.example.reuse;

import com.sstlfsj.disruptor.core.PipelineSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** demo4：在原生 topology 中显式添加清理 handler，避免槽位复用时读取残留字段。 */
@Component
public class ReusePipeline {

    private static final Logger log = LoggerFactory.getLogger(ReusePipeline.class);

    static volatile CountDownLatch latch;
    /** 记录每个订单 collect 时看到的 couponCode，供 runner 校验有无残留。 */
    static final List<String> seen = new CopyOnWriteArrayList<>();

    @Bean
    public PipelineSpec<ReuseEvent> reusePipelineSpec() {
        return PipelineSpec.builder("reuse", ReuseEvent.class, ReuseEvent::new)
                .topology(disruptor -> disruptor
                        .handleEventsWith((event, sequence, endOfBatch) -> collect(event))
                        .then((event, sequence, endOfBatch) -> event.reset()))
                .build();
    }

    public void collect(ReuseEvent e) {
        seen.add(e.getOrderId() + "=" + e.getCouponCode());
        log.info("[reuse/collect] 订单 {} couponCode={}", e.getOrderId(), e.getCouponCode());
        latch.countDown();
    }
}
