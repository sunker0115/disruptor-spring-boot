package com.sstlfsj.disruptor.example.reuse;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** demo4：只设部分字段时，Resettable 在叶子后清空，避免复用槽位读到上一轮残留。 */
@Component
public class ReusePipeline {

    private static final Logger log = LoggerFactory.getLogger(ReusePipeline.class);

    static volatile CountDownLatch latch;
    /** 记录每个订单 collect 时看到的 couponCode，供 runner 校验有无残留。 */
    static final List<String> seen = new CopyOnWriteArrayList<>();

    @DisruptorStage(pipeline = "reuse", name = "collect")
    public void collect(ReuseEvent e) {
        seen.add(e.getOrderId() + "=" + e.getCouponCode());
        log.info("[reuse/collect] 订单 {} couponCode={}", e.getOrderId(), e.getCouponCode());
        latch.countDown();
    }
}
