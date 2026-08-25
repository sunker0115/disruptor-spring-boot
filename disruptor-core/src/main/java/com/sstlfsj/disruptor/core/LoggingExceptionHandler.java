package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 SLF4J 的默认异常处理器,替代 Disruptor 内置基于 JDK {@code System.Logger} 的
 * {@code FatalExceptionHandler}。由 {@link ErrorStrategy} 装配。
 *
 * <p>{@code rethrow=false}({@link ErrorStrategy#LOG_AND_CONTINUE}):记录后吞掉异常,Disruptor 跳过出错
 * 事件、继续消费后续事件。{@code rethrow=true}({@link ErrorStrategy#HALT}):记录后抛出,终止该消费者。</p>
 *
 * <p>生命周期回调异常(onStart/onShutdown)一律只记录、不抛,避免影响管道启停。</p>
 */
final class LoggingExceptionHandler implements ExceptionHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    private final boolean rethrow;

    LoggingExceptionHandler(boolean rethrow) {
        this.rethrow = rethrow;
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
        log.error("[disruptor/error] 处理事件失败 sequence={} event={}", sequence, event, ex);
        if (rethrow) {
            throw new RuntimeException("Disruptor 消费异常(HALT 策略):sequence=" + sequence, ex);
        }
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("[disruptor/error] 消费者启动回调(onStart)失败", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("[disruptor/error] 消费者关闭回调(onShutdown)失败", ex);
    }
}
