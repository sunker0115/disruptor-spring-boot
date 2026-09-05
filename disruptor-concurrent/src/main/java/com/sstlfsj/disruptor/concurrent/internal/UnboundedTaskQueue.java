package com.sstlfsj.disruptor.concurrent.internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/** 分段、池化且以绝对发布代际保护复用的无界 MPSC typed-slot 队列。 */
final class UnboundedTaskQueue implements TaskQueue {

    private static final int MAX_POOLED_SEGMENTS = 8;
    private static final int EMPTY = -1;
    private static final long UNPUBLISHED = Long.MIN_VALUE;
    private static final TaskType[] TASK_TYPES = TaskType.values();
    private static final OrdinaryState[] ORDINARY_STATES = OrdinaryState.values();
    private static final VarHandle PUBLISHED_SEQUENCE;
    private static final VarHandle ORDINARY_STATE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PUBLISHED_SEQUENCE = lookup.findVarHandle(Cell.class, "publishedSequence", long.class);
            ORDINARY_STATE = lookup.findVarHandle(Cell.class, "ordinaryState", int.class);
        } catch (NoSuchFieldException | IllegalAccessException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private final int segmentSize;
    private final int segmentShift;
    private final int segmentMask;
    private final AtomicLong nextClaimSequence = new AtomicLong();
    private final AtomicInteger allocatedSegments = new AtomicInteger(1);
    private final AtomicInteger activeSegments = new AtomicInteger(1);
    private final ReentrantLock segmentLock = new ReentrantLock();
    private final ArrayDeque<Segment> pooledSegments = new ArrayDeque<>();

    private volatile Segment head;
    private volatile Segment tail;
    private volatile long consumerSequence = -1;
    private Cell currentCell;
    private long currentSequence = -1;

    UnboundedTaskQueue(int segmentSize) {
        if (segmentSize <= 0 || Integer.bitCount(segmentSize) != 1) {
            throw new IllegalArgumentException(
                    "segmentSize 必须为 2 的幂，实际值=" + segmentSize);
        }
        this.segmentSize = segmentSize;
        this.segmentShift = Integer.numberOfTrailingZeros(segmentSize);
        this.segmentMask = segmentSize - 1;
        Segment initial = new Segment(0, segmentSize);
        head = initial;
        tail = initial;
    }

    @Override
    public long tryClaim() {
        long sequence = nextClaimSequence.getAndIncrement();
        if (sequence < 0) {
            throw new IllegalStateException("TaskQueue sequence 已耗尽");
        }
        segmentForClaim(sequence);
        return sequence;
    }

    @Override
    public void writeOrdinary(long sequence, Runnable task) {
        Cell cell = cellForClaim(sequence);
        cell.tracked = null;
        cell.ordinary = Objects.requireNonNull(task, "task 不能为空");
        cell.ordinaryState = OrdinaryState.WAITING.ordinal();
        cell.type = TaskType.ORDINARY.ordinal();
    }

    @Override
    public void writeTracked(long sequence, AcceptedTask<?> record) {
        writeRecord(sequence, record, TaskType.TRACKED);
    }

    @Override
    public void writeSchedule(long sequence, AcceptedTask<?> record) {
        writeRecord(sequence, record, TaskType.SCHEDULE);
    }

    @Override
    public void writeTombstone(long sequence) {
        Cell cell = cellForClaim(sequence);
        cell.ordinary = null;
        cell.tracked = null;
        cell.ordinaryState = OrdinaryState.TERMINAL.ordinal();
        cell.type = TaskType.TOMBSTONE.ordinal();
    }

    @Override
    public void publish(long sequence) {
        Cell cell = cellForClaim(sequence);
        PUBLISHED_SEQUENCE.setRelease(cell, sequence);
    }

    @Override
    public boolean poll() {
        if (currentCell != null) {
            return true;
        }
        long next = consumerSequence + 1;
        Segment segment = segmentForConsumer(next);
        if (segment == null) {
            return false;
        }
        Cell cell = segment.cells[(int) next & segmentMask];
        if ((long) PUBLISHED_SEQUENCE.getAcquire(cell) != next) {
            return false;
        }
        if (cell.type == EMPTY) {
            throw new IllegalStateException("已发布的无界槽位没有类型");
        }
        currentSequence = next;
        currentCell = cell;
        return true;
    }

    @Override
    public TaskType currentType() {
        return TASK_TYPES[current().type];
    }

    @Override
    public Runnable currentOrdinary() {
        requireCurrentType(TaskType.ORDINARY);
        return current().ordinary;
    }

    @Override
    public AcceptedTask<?> currentRecord() {
        TaskType type = currentType();
        if (type != TaskType.TRACKED && type != TaskType.SCHEDULE) {
            throw new IllegalStateException("当前槽位不是 tracked/schedule：" + type);
        }
        return current().tracked;
    }

    @Override
    public boolean tryStartCurrentOrdinary() {
        requireCurrentType(TaskType.ORDINARY);
        return ORDINARY_STATE.compareAndSet(current(),
                OrdinaryState.WAITING.ordinal(), OrdinaryState.RUNNING.ordinal());
    }

    @Override
    public OrdinaryState currentOrdinaryState() {
        requireCurrentType(TaskType.ORDINARY);
        return ORDINARY_STATES[(int) ORDINARY_STATE.getAcquire(current())];
    }

    @Override
    public void terminalizeCurrentOrdinary() {
        requireCurrentType(TaskType.ORDINARY);
        if (!ORDINARY_STATE.compareAndSet(current(),
                OrdinaryState.RUNNING.ordinal(), OrdinaryState.TERMINAL.ordinal())) {
            throw new IllegalStateException("只有 RUNNING ordinary 可以终结");
        }
    }

    @Override
    public void advanceConsumer() {
        current();
        consumerSequence = currentSequence;
    }

    @Override
    public void releaseCurrentSlot() {
        Cell cell = current();
        cell.ordinary = null;
        cell.tracked = null;
        cell.type = EMPTY;
        currentCell = null;
    }

    @Override
    public long claimedCursor() {
        return nextClaimSequence.get() - 1;
    }

    @Override
    public long pending() {
        return claimedCursor() - consumerSequence;
    }

    @Override
    public void scanOrdinaryUnstarted(
            long claimedInclusive,
            OrdinaryDisposition disposition,
            OrdinaryClaimedSink sink) {
        Objects.requireNonNull(disposition, "disposition 不能为空");
        Objects.requireNonNull(sink, "sink 不能为空");
        int target = disposition == OrdinaryDisposition.RETURN
                ? OrdinaryState.RETURNED.ordinal()
                : OrdinaryState.DISCARDED.ordinal();
        segmentLock.lock();
        try {
            long first = consumerSequence + 1;
            Segment segment = head;
            for (long sequence = first; sequence <= claimedInclusive; sequence++) {
                long expectedSegmentId = sequence >>> segmentShift;
                while (segment != null && segment.id < expectedSegmentId) {
                    segment = segment.next;
                }
                if (segment == null || segment.id != expectedSegmentId) {
                    continue;
                }
                Cell cell = segment.cells[(int) sequence & segmentMask];
                if ((long) PUBLISHED_SEQUENCE.getAcquire(cell) != sequence
                        || cell.type != TaskType.ORDINARY.ordinal()) {
                    continue;
                }
                Runnable original = cell.ordinary;
                if (original != null && ORDINARY_STATE.compareAndSet(cell,
                        OrdinaryState.WAITING.ordinal(), target)) {
                    sink.accept(sequence, original);
                }
            }
        } finally {
            segmentLock.unlock();
        }
    }

    @Override
    public int allocatedSegments() {
        return allocatedSegments.get();
    }

    @Override
    public int activeSegments() {
        return activeSegments.get();
    }

    int retainedReferences() {
        int retained = 0;
        segmentLock.lock();
        try {
            Segment segment = head;
            while (segment != null) {
                for (Cell cell : segment.cells) {
                    if (cell.ordinary != null || cell.tracked != null) {
                        retained++;
                    }
                }
                segment = segment.next;
            }
            return retained;
        } finally {
            segmentLock.unlock();
        }
    }

    private void writeRecord(long sequence, AcceptedTask<?> record, TaskType type) {
        Cell cell = cellForClaim(sequence);
        cell.ordinary = null;
        cell.tracked = Objects.requireNonNull(record, "record 不能为空");
        cell.ordinaryState = OrdinaryState.TERMINAL.ordinal();
        cell.type = type.ordinal();
    }

    private Cell cellForClaim(long sequence) {
        Segment segment = segmentForClaim(sequence);
        return segment.cells[(int) sequence & segmentMask];
    }

    private Segment segmentForClaim(long sequence) {
        long targetId = sequence >>> segmentShift;
        Segment observedTail = tail;
        if (observedTail.id == targetId) {
            return observedTail;
        }
        if (observedTail.id > targetId) {
            return findFromHead(targetId);
        }
        segmentLock.lock();
        try {
            Segment current = tail;
            while (current.id < targetId) {
                Segment next = acquireSegment(current.id + 1);
                current.next = next;
                current = next;
                tail = current;
                activeSegments.incrementAndGet();
            }
            return current;
        } finally {
            segmentLock.unlock();
        }
    }

    private Segment findFromHead(long targetId) {
        Segment segment = head;
        while (segment.id < targetId) {
            segment = segment.next;
            if (segment == null) {
                return segmentForClaim(targetId << segmentShift);
            }
        }
        if (segment.id != targetId) {
            throw new IllegalStateException("claim sequence 落后于已回收 segment");
        }
        return segment;
    }

    private Segment segmentForConsumer(long sequence) {
        long targetId = sequence >>> segmentShift;
        Segment current = head;
        if (current.id == targetId) {
            return current;
        }
        if (current.id > targetId) {
            throw new IllegalStateException("consumer sequence 落后于 head segment");
        }
        segmentLock.lock();
        try {
            current = head;
            while (current.id < targetId) {
                Segment next = current.next;
                if (next == null) {
                    return null;
                }
                head = next;
                recycleSegment(current);
                activeSegments.decrementAndGet();
                current = next;
            }
            return current.id == targetId ? current : null;
        } finally {
            segmentLock.unlock();
        }
    }

    private Segment acquireSegment(long id) {
        Segment segment = pooledSegments.pollFirst();
        if (segment == null) {
            allocatedSegments.incrementAndGet();
            return new Segment(id, segmentSize);
        }
        segment.reset(id);
        return segment;
    }

    private void recycleSegment(Segment segment) {
        segment.next = null;
        if (pooledSegments.size() < MAX_POOLED_SEGMENTS) {
            pooledSegments.addFirst(segment);
        } else {
            allocatedSegments.decrementAndGet();
        }
    }

    private Cell current() {
        if (currentCell == null) {
            throw new IllegalStateException("当前没有已轮询槽位");
        }
        return currentCell;
    }

    private void requireCurrentType(TaskType expected) {
        TaskType actual = currentType();
        if (actual != expected) {
            throw new IllegalStateException("当前槽位类型应为 " + expected + "，实际=" + actual);
        }
    }

    private static final class Segment {
        private long id;
        private final Cell[] cells;
        private volatile Segment next;

        private Segment(long id, int segmentSize) {
            this.id = id;
            this.cells = new Cell[segmentSize];
            for (int index = 0; index < segmentSize; index++) {
                cells[index] = new Cell();
            }
        }

        private void reset(long id) {
            this.id = id;
            this.next = null;
            for (Cell cell : cells) {
                cell.type = EMPTY;
                cell.ordinaryState = OrdinaryState.TERMINAL.ordinal();
                cell.ordinary = null;
                cell.tracked = null;
                cell.publishedSequence = UNPUBLISHED;
            }
        }
    }

    private static final class Cell {
        private int type = EMPTY;
        private int ordinaryState = OrdinaryState.TERMINAL.ordinal();
        private Runnable ordinary;
        private AcceptedTask<?> tracked;
        private volatile long publishedSequence = UNPUBLISHED;
    }
}
