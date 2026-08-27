package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.ExceptionHandler;

/**
 * 管道消费异常的默认处置策略(全局级,作用于一条管道的所有消费者)。映射为 Disruptor 的默认
 * {@link ExceptionHandler}。
 *
 * <p>默认 {@link #HALT} 优先保护链式拓扑的一致性：handler 抛异常后终止该消费者，序列不再推进。
 * {@link #LOG_AND_CONTINUE} 适用于终端消费者、幂等处理或业务明确接受部分处理的场景；它会吞掉异常并
 * 推进当前消费者序列，因此依赖它的下游仍会收到失败槽位。</p>
 *
 * <p>需要完全自定义处理逻辑(按事件类型、单条重试计数、投递失败通道等)时,走
 * {@code PipelineSpec.exceptionHandler(...)} 编程式逃生口;per-handler 差异化处理走 topology 里的
 * {@code disruptor.handleExceptionsFor(...).with(...)}。</p>
 */
public enum ErrorStrategy {

    /** 记录 ERROR 日志并跳过出错事件，继续消费；当前消费者序列推进，下游仍会收到失败槽位。 */
    LOG_AND_CONTINUE,

    /**
     * 记录 ERROR 日志并向上抛出:该消费者线程终止、其序列不再推进,依赖它的下游与背压随之停滞。
     * 默认策略；须配合监控告警和人工恢复使用。
     */
    HALT;

    /** 产出本策略对应的 SLF4J 异常处理器。事件类型无关,故用 {@code ExceptionHandler<Object>}。 */
    public ExceptionHandler<Object> handler() {
        return new LoggingExceptionHandler(this == HALT);
    }
}
