package com.sstlfsj.disruptor.event;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 兜底异常处理器：替换 Disruptor 4.0 默认的 {@code FatalExceptionHandler}
 * （其 {@code handleEventException} 会 rethrow，从而杀死消费线程）。
 *
 * <p>三个回调都仅用 SLF4J 记 ERROR 且不再抛出，保证任何未被
 * {@link ConsumerRegistry#dispatch} 内隔离掉的异常也不会终止消费线程、
 * 导致事件总线永久停摆。与 dispatch 内的 per-consumer try-catch 职责互补：
 * 后者保证同一事件的其它消费者不受牵连，本处保证消费线程存活。</p>
 */
public class LoggingExceptionHandler implements ExceptionHandler<EventWrapper> {

    private static final Logger log = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    @Override
    public void handleEventException(Throwable ex, long sequence, EventWrapper event) {
        Object payload = event == null ? null : event.getPayload();
        log.error("处理事件异常，sequence={}，payload={}，已忽略以保持消费线程存活", sequence, payload, ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("Disruptor onStart 异常", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("Disruptor onShutdown 异常", ex);
    }
}
