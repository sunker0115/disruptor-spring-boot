package com.sstlfsj.disruptor.example.pay;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * demo2：自定义管道级 {@link ExceptionHandler}。
 *
 * <p>默认 LMAX 策略（FatalExceptionHandler）会重新抛出异常、中断消费线程；
 * 这里接住异常、记录后不 rethrow，Disruptor 的 BatchEventProcessor 会继续处理下一个事件，
 * 从而证明单个事件失败不会拖垮整条管道。
 */
@Component
public class PayExceptionHandler implements ExceptionHandler<PayEvent> {

    private static final Logger log = LoggerFactory.getLogger(PayExceptionHandler.class);

    @Override
    public void handleEventException(Throwable ex, long sequence, PayEvent event) {
        log.warn("[pay/error] 支付 {} 处理失败，已被自定义 ExceptionHandler 接住，管道继续：{}",
                event == null ? "?" : event.getPayId(), ex.getMessage());
        PayService.latch.countDown();
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("[pay] 启动异常", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("[pay] 关闭异常", ex);
    }
}
