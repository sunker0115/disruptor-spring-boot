package com.sstlfsj.disruptor.concurrent.internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** 分段、池化且以绝对发布代际保护复用的无界 MPSC typed-slot 队列。 */
final class UnboundedTaskQueue implements TaskQueue {

    private static final int EMPTY = -1;
    private static final long UNPUBLISHED = Long.MIN_VALUE;
    private static final TaskType[] TASK_TYPES = TaskType.values();
    private static final OrdinaryState[] ORDINARY_STATES = OrdinaryState.values();
    private static final VarHandle PUBLISHED_SEQUENCE;
    private static final VarHandle ORDINARY_STATE;
    private static final VarHandle NEXT;
    private static final VarHandle TAIL;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PUBLISHED_SEQUENCE = lookup.findVarHandle(Cell.class, "publishedSequence", long.class);
            ORDINARY_STATE = lookup.findVarHandle(Cell.class, "ordinaryState", int.class);
            NEXT = lookup.findVarHandle(Segment.class, "next", Segment.class);
            TAIL = lookup.findVarHandle(UnboundedTaskQueue.class, "tail", Segment.class);
        } catch (NoSuchFieldException | IllegalAccessException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private final int segmentSize;
    private final int segmentShift;
    private final int segmentMask;
    private final int maxPooledSegments;
    private final SegmentFactory segmentFactory;
    private final UnboundedTaskLedger ledger = new UnboundedTaskLedger();
    private final AtomicInteger allocatedSegments = new AtomicInteger(1);
    private final AtomicReferenceArray<Cell[]> pooledCells;

    private volatile Segment head;
    private volatile Segment tail;
    private volatile long consumerSequence = -1;
    private Cell currentCell;
    private long currentSequence = -1;

    UnboundedTaskQueue(int segmentSize, int maxPooledSegments) {
        this(segmentSize, maxPooledSegments, Segment::new);
    }

    UnboundedTaskQueue(int segmentSize, int maxPooledSegments, SegmentFactory segmentFactory) {
        if (segmentSize <= 0 || Integer.bitCount(segmentSize) != 1) {
            throw new IllegalArgumentException(
                    "segmentSize 必须为 2 的幂，实际值=" + segmentSize);
        }
        if (maxPooledSegments < 0) {
            throw new IllegalArgumentException(
                    "maxPooledSegments 不能为负数，实际值=" + maxPooledSegments);
        }
        this.segmentSize = segmentSize;
        this.segmentShift = Integer.numberOfTrailingZeros(segmentSize);
        this.segmentMask = segmentSize - 1;
        this.maxPooledSegments = maxPooledSegments;
        this.pooledCells = new AtomicReferenceArray<>(maxPooledSegments);
        this.segmentFactory = Objects.requireNonNull(segmentFactory, "segmentFactory 不能为空");
        Segment initial = segmentFactory.create(0, segmentSize);
        head = initial;
        tail = initial;
    }

    @Override
    public long tryClaim() {
        while (true) {
            long sequence = ledger.candidateSequence();
            // 分配可失败；只有目标段已就绪才能提交占号。
            if (segmentForClaim(sequence, true) != null && ledger.tryCommitClaim(sequence)) {
                return sequence;
            }
        }
    }

    UnboundedTaskLedger ledger() {
        return ledger;
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
        // type/state/payload 的完整 plain 写先于此 release；只有 acquire 精确读到
        // 同一绝对 sequence 的 worker/scanner 才能读槽。复用不清除旧 publication。
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
        return ledger.claimedCursor();
    }

    @Override
    public long consumerCursor() {
        return consumerSequence;
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
        // kernel 已封 admission、drain publishers 并取得静默票据。
        // 扫描只读稳定链接/游标，只有 WAITING ownership 通过 CAS 改变。
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
    }

    @Override
    public QueueSegmentSnapshot segmentSnapshot() {
        while (true) {
            Segment first = head;
            Segment last = tail;
            int allocated = allocatedSegments.get();
            // 节点身份永不复用，双读不会 ABA；稳定后精确，并发时允许漏计尚未
            // 推进 tail 的刚链接段。allocated 还包括池及 producer 独占的候选段。
            if (first == head && last == tail) {
                return new QueueSegmentSnapshot(allocated, (int) (last.id - first.id + 1));
            }
        }
    }

    int retainedReferences() {
        int retained = 0;
        // 诊断仅在静默期调用，与 scanner 一样不参与结构同步。
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
        return segmentForClaim(sequence, false);
    }

    private Segment segmentForClaim(long sequence, boolean candidate) {
        long targetId = sequence >>> segmentShift;
        while (true) {
            Segment current = tail;
            if (current.id > targetId) {
                current = head;
            }
            while (current.id < targetId) {
                Segment next = current.next;
                if (next == null) {
                    if (candidate && ledger.candidateSequence() != sequence) {
                        return null;
                    }
                    Segment created = acquireSegment(current.id + 1);
                    if (NEXT.compareAndSet(current, null, created)) {
                        next = created;
                    } else {
                        recycleSegment(created);
                        next = current.next;
                    }
                }
                // 链接赢家可以暂停；任何观察到 next 的 producer 都能帮助推进 tail。
                TAIL.compareAndSet(this, current, next);
                current = next;
            }
            if (current.id == targetId) {
                // 已提交的占号由 publication hole 保护；候选号即使已过期也只能
                // 在 ledger CAS 失败后重试，不能触碰 Cell。
                return current;
            }
            if (candidate && ledger.candidateSequence() != sequence) {
                return null;
            }
        }
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
        // poll 只有在 currentCell 已 release 后才进入此处；唯一 consumer 已顺序
        // 释放整段所有槽。段链接后永不改写，关闭 scanner 可沿旧 head 安全遍历；
        // gate 排空 producer 后，回收到池中的 Cell[] 也不会在扫描期间再次分配。
        while (current.id < targetId) {
            Segment next = current.next;
            if (next == null) {
                return null;
            }
            TAIL.compareAndSet(this, current, next);
            head = next;
            recycleSegment(current);
            current = next;
        }
        return current.id == targetId ? current : null;
    }

    private Segment acquireSegment(long id) {
        for (int index = 0; index < maxPooledSegments; index++) {
            Cell[] cells = pooledCells.getAndSet(index, null);
            if (cells != null) {
                try {
                    // 只复用 Cell 存储，节点的 id/身份和已链接 next 永不复用或改写。
                    return new Segment(id, cells);
                } catch (Throwable failure) {
                    recycleCells(cells);
                    throw failure;
                }
            }
        }
        Segment created = segmentFactory.create(id, segmentSize);
        allocatedSegments.incrementAndGet();
        return created;
    }

    private void recycleSegment(Segment segment) {
        recycleCells(segment.cells);
    }

    private void recycleCells(Cell[] cells) {
        for (int index = 0; index < maxPooledSegments; index++) {
            // CAS release 传递 worker 清槽；getAndSet acquire 唯一取得存储所有权。
            if (pooledCells.compareAndSet(index, null, cells)) {
                return;
            }
        }
        allocatedSegments.decrementAndGet();
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

    @FunctionalInterface
    interface SegmentFactory {
        Segment create(long id, int segmentSize);
    }

    static final class Segment {
        private final long id;
        private final Cell[] cells;
        private volatile Segment next;

        Segment(long id, int segmentSize) {
            this.id = id;
            this.cells = new Cell[segmentSize];
            for (int index = 0; index < segmentSize; index++) {
                cells[index] = new Cell();
            }
        }

        private Segment(long id, Cell[] cells) {
            this.id = id;
            this.cells = cells;
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
