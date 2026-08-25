package com.sstlfsj.disruptor.example.pay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
public class PayService {

    /** 由 runner 注入本轮 latch：正常扣款 countDown，异常被 PayExceptionHandler 接住时也 countDown。 */
    static volatile CountDownLatch latch;

    private static final Logger log = LoggerFactory.getLogger(PayService.class);

    /** 金额非正视为非法支付，抛异常交给自定义 ExceptionHandler 处理，而非让默认策略中断管道。 */
    public void charge(PayEvent e) {
        if (e.getAmount() <= 0) {
            throw new IllegalArgumentException(
                    "非法支付金额 " + e.getAmount() + "（payId=" + e.getPayId() + "）");
        }
        log.info("[pay/charge] 支付 {} 扣款 {} 成功", e.getPayId(), e.getAmount());
        latch.countDown();
    }
}
