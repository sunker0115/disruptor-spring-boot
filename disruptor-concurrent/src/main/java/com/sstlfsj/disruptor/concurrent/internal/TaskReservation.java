package com.sstlfsj.disruptor.concurrent.internal;

/** 一个必须 publish 或 abort 的队列序号保留。 */
interface TaskReservation {

    long sequence();

    void publish(AcceptedTask<?> task);

    void abort();
}
