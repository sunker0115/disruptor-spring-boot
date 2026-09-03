package com.sstlfsj.disruptor.core;

import java.util.concurrent.CompletionStage;

/** 一条可独立启动、监督、关闭和观察的 Disruptor 管道。 */
public interface DisruptorPipeline<E> {

    PipelineHandle<E> handle();

    PipelineSnapshot snapshot();

    CompletionStage<Void> start();

    void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline);

    CompletionStage<PipelineSnapshot> termination();
}
