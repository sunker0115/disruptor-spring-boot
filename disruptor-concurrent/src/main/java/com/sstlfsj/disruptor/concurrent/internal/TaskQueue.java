package com.sstlfsj.disruptor.concurrent.internal;

/** 多生产者保留/发布、单消费者轮询的唯一可替换内核边界。 */
interface TaskQueue {

    TaskReservation tryReserve();

    AcceptedTask<?> poll();

    long pending();

    long remainingCapacity();

    int allocatedSegments();
}
