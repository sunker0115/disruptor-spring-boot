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

    long consumerCursor();

    /** 已 claim 且尚未离开 ingress 的槽数，包含 publication hole，不含 executing。 */
    default long pending() {
        // consumer 的 release 进度只来自已 claim 的槽；先 acquire 它，再读 claimed
        // 保证后一次读取不能落在已观测消费进度之前，且不把 executing 计入 ingress。
        long consumed = consumerCursor();
        return claimedCursor() - consumed;
    }

    void scanOrdinaryUnstarted(
            long claimedInclusive,
            OrdinaryDisposition disposition,
            OrdinaryClaimedSink sink);

    /** 在同一个队列一致性边界内读取物理保留段与活跃段。 */
    QueueSegmentSnapshot segmentSnapshot();
}

record QueueSegmentSnapshot(int allocated, int active) {

    static final QueueSegmentSnapshot NONE = new QueueSegmentSnapshot(0, 0);

    QueueSegmentSnapshot {
        if (allocated < 0 || active < 0 || active > allocated) {
            throw new IllegalArgumentException(
                    "segment 计数非法：allocated=" + allocated + "，active=" + active);
        }
    }
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
