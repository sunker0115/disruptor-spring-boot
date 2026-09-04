package com.sstlfsj.disruptor.concurrent;

import java.util.concurrent.ScheduledFuture;

/** 可查询调度事实快照的 JDK ScheduledFuture。 */
public interface EventLoopScheduledFuture<V> extends ScheduledFuture<V> {

    ScheduledTaskSnapshot snapshot();
}
