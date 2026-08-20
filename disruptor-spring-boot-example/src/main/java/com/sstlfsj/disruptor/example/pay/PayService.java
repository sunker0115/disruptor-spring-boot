package com.sstlfsj.disruptor.example.pay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class PayService {

    private static final Logger log = LoggerFactory.getLogger(PayService.class);

    /** 由 runner 注入本轮 latch（4 阶段各 countDown 一次）。 */
    static volatile CountDownLatch latch;

    public void validate(PayEvent e) {
        log.info("[pay/validate] 支付 {} 校验通过", e.getPayId());
        latch.countDown();
    }

    public void persist(PayEvent e) {
        e.setPersisted(true);
        log.info("[pay/persist] 支付 {} 已落库", e.getPayId());
        latch.countDown();
    }

    public void audit(PayEvent e) {
        e.setAudited(true);
        log.info("[pay/audit] 支付 {} 已审计", e.getPayId());
        latch.countDown();
    }

    public void notify(PayEvent e) {
        log.info("[pay/notify] 支付 {} 通知（persist={}, audit={}）",
                e.getPayId(), e.isPersisted(), e.isAudited());
        latch.countDown();
    }
}
