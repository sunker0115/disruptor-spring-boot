package com.sstlfsj.disruptor.concurrent.internal;

/** 多生产者 claim/publish、单消费者消费以及关闭扫描的物理存储边界。 */
interface TaskQueue {

    long tryClaim();

    void writeOrdinary(long sequence, Runnable task);

    void writeTracked(long sequence, AcceptedTask<?> record);

    void writeSchedule(long sequence, AcceptedTask<?> record);

    void writeTombstone(long sequence);

    void publish(long sequence);

    boolean poll();

    TaskType currentType();

    Runnable currentOrdinary();

    AcceptedTask<?> currentRecord();

    boolean tryStartCurrentOrdinary();

    OrdinaryState currentOrdinaryState();

    void terminalizeCurrentOrdinary();

    void advanceConsumer();

    void releaseCurrentSlot();

    long claimedCursor();

    long pending();

    void scanOrdinaryUnstarted(
            long claimedInclusive,
            OrdinaryDisposition disposition,
            OrdinaryClaimedSink sink);

    int allocatedSegments();

    int activeSegments();
}

enum TaskType {
    ORDINARY,
    TRACKED,
    SCHEDULE,
    TOMBSTONE
}

enum OrdinaryState {
    WAITING,
    RUNNING,
    RETURNED,
    DISCARDED,
    TERMINAL
}

enum OrdinaryDisposition {
    RETURN,
    DISCARD
}

@FunctionalInterface
interface OrdinaryClaimedSink {
    void accept(long sequence, Runnable original);
}
