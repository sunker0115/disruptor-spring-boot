package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.ExceptionHandler;

/**
 * 管道消费异常的默认处置策略(全局级,作用于一条管道的所有消费者)。映射为 Disruptor 的默认
 * {@link ExceptionHandler}。
 *
 * <p>Disruptor 原生默认是 {@code FatalExceptionHandler}:handler 抛异常即 rethrow,该消费者线程终止、
 * 序列不再推进,依赖它的下游与生产者背压随之卡死。本枚举提供两档语义清晰的替代,默认
 * {@link #LOG_AND_CONTINUE} 保证进程内安全。</p>
 *
 * <p>需要完全自定义处理逻辑(按事件类型、单条重试计数、投递失败通道等)时,走
 * {@code PipelineSpec.exceptionHandler(...)} 编程式逃生口;per-handler 差异化处理走 topology 里的
 * {@code disruptor.handleExceptionsFor(...).with(...)}。</p>
 */
public enum ErrorStrategy {

    /** 记录 ERROR 日志并跳过出错事件,继续消费后续事件。进程内安全默认。 */
    LOG_AND_CONTINUE,

    /**
     * 记录 ERROR 日志并向上抛出:该消费者线程终止、其序列不再推进,依赖它的下游与背压随之停滞。
     * 仅用于"出错即停、人工介入"的严格场景,须配合监控告警使用。
     */
    HALT;

    /** 产出本策略对应的 SLF4J 异常处理器。事件类型无关,故用 {@code ExceptionHandler<Object>}。 */
    public ExceptionHandler<Object> handler() {
        return new LoggingExceptionHandler(this == HALT);
    }
}
