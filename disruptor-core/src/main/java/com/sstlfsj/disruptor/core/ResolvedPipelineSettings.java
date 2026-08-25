package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;

record ResolvedPipelineSettings<E>(
        int bufferSize,
        ProducerType producerType,
        WaitStrategy waitStrategy,
        ThreadFactory threadFactory,
        Duration shutdownTimeout,
        ExceptionHandler<? super E> exceptionHandler) {
}
