package com.sstlfsj.disruptor.concurrent.internal;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/** 使用 LMAX multi-producer RingBuffer 的有界 typed-slot 队列。 */
final class BoundedTaskQueue implements TaskQueue {

    private static final int EMPTY = -1;
    private static final TaskType[] TASK_TYPES = TaskType.values();
    private static final OrdinaryState[] ORDINARY_STATES = OrdinaryState.values();
    private static final VarHandle ORDINARY_STATE;

    static {
        try {
            ORDINARY_STATE = MethodHandles.lookup()
                    .findVarHandle(Cell.class, "ordinaryState", int.class);
        } catch (NoSuchFieldException | IllegalAccessException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private final RingBuffer<Cell> ringBuffer;
    private final Sequence consumerSequence = new Sequence(RingBuffer.INITIAL_CURSOR_VALUE);
    private final Sequence gatingSequence = new Sequence(RingBuffer.INITIAL_CURSOR_VALUE);

    private Cell currentCell;
    private long currentSequence = RingBuffer.INITIAL_CURSOR_VALUE;

    BoundedTaskQueue(int capacity) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity 必须为 2 的幂，实际值=" + capacity);
        }
        ringBuffer = RingBuffer.createMultiProducer(
                Cell::new, capacity, new BusySpinWaitStrategy());
        ringBuffer.addGatingSequences(gatingSequence);
    }

    @Override
    public long tryClaim() {
        try {
            return ringBuffer.tryNext();
        } catch (InsufficientCapacityException ignored) {
            return -1;
        }
    }

    @Override
    public void writeOrdinary(long sequence, Runnable task) {
        Cell cell = cell(sequence);
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
        Cell cell = cell(sequence);
        cell.ordinary = null;
        cell.tracked = null;
        cell.ordinaryState = OrdinaryState.TERMINAL.ordinal();
        cell.type = TaskType.TOMBSTONE.ordinal();
    }

    @Override
    public void publish(long sequence) {
        ringBuffer.publish(sequence);
    }

    @Override
    public boolean poll() {
        if (currentCell != null) {
            return true;
        }
        long next = consumerSequence.get() + 1;
        if (!ringBuffer.isAvailable(next)) {
            return false;
        }
        currentSequence = next;
        currentCell = cell(next);
        if (currentCell.type == EMPTY) {
            throw new IllegalStateException("已发布的 RingBuffer 槽位没有类型");
        }
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
        consumerSequence.set(currentSequence);
    }

    @Override
    public void releaseCurrentSlot() {
        Cell cell = current();
        cell.ordinary = null;
        cell.tracked = null;
        cell.type = EMPTY;
        gatingSequence.set(currentSequence);
        currentCell = null;
    }

    @Override
    public long claimedCursor() {
        return ringBuffer.getCursor();
    }

    @Override
    public long consumerCursor() {
        return consumerSequence.get();
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
        long first = consumerSequence.get() + 1;
        for (long sequence = first; sequence <= claimedInclusive; sequence++) {
            if (!ringBuffer.isAvailable(sequence)) {
                continue;
            }
            Cell cell = cell(sequence);
            if (cell.type != TaskType.ORDINARY.ordinal()) {
                continue;
            }
            Runnable original = cell.ordinary;
            if (original != null && ORDINARY_STATE.compareAndSet(cell,
                    OrdinaryState.WAITING.ordinal(), target)) {
                sink.accept(sequence, original);
            }
        }
    }

    @Override
    public int allocatedSegments() {
        return 0;
    }

    @Override
    public int activeSegments() {
        return 0;
    }

    int retainedReferences() {
        int retained = 0;
        for (int index = 0; index < ringBuffer.getBufferSize(); index++) {
            Cell cell = ringBuffer.get(index);
            if (cell.ordinary != null || cell.tracked != null) {
                retained++;
            }
        }
        return retained;
    }

    private void writeRecord(long sequence, AcceptedTask<?> record, TaskType type) {
        Cell cell = cell(sequence);
        cell.ordinary = null;
        cell.tracked = Objects.requireNonNull(record, "record 不能为空");
        cell.ordinaryState = OrdinaryState.TERMINAL.ordinal();
        cell.type = type.ordinal();
    }

    private Cell cell(long sequence) {
        return ringBuffer.get(sequence);
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

    private static final class Cell {
        private int type = EMPTY;
        private int ordinaryState = OrdinaryState.TERMINAL.ordinal();
        private Runnable ordinary;
        private AcceptedTask<?> tracked;
    }
}
