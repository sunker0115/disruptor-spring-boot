package com.sstlfsj.disruptor.example.order;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/** demo1：validate → (persist ‖ audit) → notify。persist/audit 写不同字段避免竞态；notify 汇聚。 */
@Component
public class OrderPipeline {

    private static final Logger log = LoggerFactory.getLogger(OrderPipeline.class);

    /** 由 runner 注入本轮 latch（4 个阶段各 countDown 一次）。 */
    static volatile CountDownLatch latch;

    @DisruptorStage(pipeline = "order", name = "validate")
    public void validate(OrderEvent e) {
        log.info("[order/validate] 订单 {} 金额 {} 校验通过", e.getOrderId(), e.getAmount());
        latch.countDown();
    }

    @DisruptorStage(pipeline = "order", name = "persist", after = "validate")
    public void persist(OrderEvent e) {
        e.setPersisted(true);
        log.info("[order/persist] 订单 {} 已落库", e.getOrderId());
        latch.countDown();
    }

    @DisruptorStage(pipeline = "order", name = "audit", after = "validate")
    public void audit(OrderEvent e) {
        e.setAudited(true);
        log.info("[order/audit] 订单 {} 已审计", e.getOrderId());
        latch.countDown();
    }

    @DisruptorStage(pipeline = "order", name = "notify", after = {"persist", "audit"})
    public void notify(OrderEvent e) {
        log.info("[order/notify] 订单 {} 通知（persist={}, audit={}，两分支都完成后才执行）",
                e.getOrderId(), e.isPersisted(), e.isAudited());
        latch.countDown();
    }
}
