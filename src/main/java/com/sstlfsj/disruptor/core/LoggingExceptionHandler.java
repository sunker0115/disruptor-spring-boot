package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 兜底异常处理器：替换 Disruptor 4.0 默认的 {@code FatalExceptionHandler}（其
 * {@code handleEventException} 会 rethrow，杀死消费线程）。三个回调都仅用 SLF4J 记 ERROR
 * 且不再抛出，保证任一阶段处理异常都不会终止消费线程、导致管道停摆。
 *
 * <p>声明为 {@code ExceptionHandler<Object>} 以复用于任意事件类型的 Disruptor
 * （{@code setDefaultExceptionHandler} 接受 {@code ExceptionHandler<? super E>}）。</p>
 */
public class LoggingExceptionHandler implements ExceptionHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
        log.error("处理事件异常，sequence={}，event={}，已忽略以保持消费线程存活", sequence, event, ex);
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
