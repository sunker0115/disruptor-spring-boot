package com.sstlfsj.disruptor.concurrent;

import java.time.Duration;

/** 依据上一次执行事实计算下一次延迟。 */
@FunctionalInterface
public interface DynamicDelay {

    Duration nextDelay(ScheduledTaskSnapshot lastRunSnapshot) throws Exception;
}
