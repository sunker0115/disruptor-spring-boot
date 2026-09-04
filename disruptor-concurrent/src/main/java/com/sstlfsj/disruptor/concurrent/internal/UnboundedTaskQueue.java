package com.sstlfsj.disruptor.concurrent.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** 按全局 sequence 保留、分段 release/acquire 发布的无界 MPSC TaskQueue。 */
final class UnboundedTaskQueue implements TaskQueue {

    private final int segmentSize;
    private final int segmentShift;
    private final int segmentMask;
    private final AtomicLong nextReservationSequence = new AtomicLong();
    private final AtomicLong pending = new AtomicLong();
    private final AtomicInteger allocatedSegments = new AtomicInteger(1);

    private volatile Segment head;
    private long nextConsumerSequence;

    UnboundedTaskQueue(int segmentSize) {
        if (segmentSize <= 0 || Integer.bitCount(segmentSize) != 1) {
            throw new IllegalArgumentException("segmentSize 必须为 2 的幂，实际值=" + segmentSize);
        }
        this.segmentSize = segmentSize;
        this.segmentShift = Integer.numberOfTrailingZeros(segmentSize);
        this.segmentMask = segmentSize - 1;
        this.head = new Segment(0, segmentSize);
    }

    @Override
    public TaskReservation tryReserve() {
        long sequence = nextReservationSequence.getAndIncrement();
        if (sequence < 0) {
            throw new IllegalStateException("TaskQueue sequence 已耗尽");
        }
        Segment segment = segmentFor(sequence);
        pending.incrementAndGet();
        return new Reservation(sequence, segment, (int) sequence & segmentMask);
    }

    @Override
    public AcceptedTask<?> poll() {
        while (true) {
            Segment segment = head;
            long expectedSegmentId = nextConsumerSequence >>> segmentShift;
            while (segment.id < expectedSegmentId) {
                Segment next = segment.next.get();
                if (next == null) {
                    return null;
                }
                head = next;
                allocatedSegments.decrementAndGet();
                segment = next;
            }
            if (segment.id > expectedSegmentId) {
                throw new IllegalStateException("消费者 segment 与 sequence 不一致");
            }
            int offset = (int) nextConsumerSequence & segmentMask;
            TaskEnvelope envelope = segment.slots.get(offset);
            if (envelope == null) {
                return null;
            }
            segment.slots.set(offset, null);
            nextConsumerSequence++;
            pending.decrementAndGet();
            if (offset == segmentMask) {
                Segment next = segment.next.get();
                if (next != null) {
                    head = next;
                    allocatedSegments.decrementAndGet();
                }
            }
            if (!envelope.tombstone()) {
                return envelope.task();
            }
        }
    }

    @Override
    public long pending() {
        return pending.get();
    }

    @Override
    public long remainingCapacity() {
        return Long.MAX_VALUE;
    }

    @Override
    public int allocatedSegments() {
        return allocatedSegments.get();
    }

    int retainedReferences() {
        int retained = 0;
        Segment segment = head;
        while (segment != null) {
            for (int index = 0; index < segmentSize; index++) {
                if (segment.slots.get(index) != null) {
                    retained++;
                }
            }
            segment = segment.next.get();
        }
        return retained;
    }

    private Segment segmentFor(long sequence) {
        long segmentId = sequence >>> segmentShift;
        Segment segment = head;
        if (segmentId < segment.id) {
            throw new IllegalStateException("保留 sequence 落后于已回收 segment");
        }
        while (segment.id < segmentId) {
            Segment next = segment.next.get();
            if (next == null) {
                Segment candidate = new Segment(segment.id + 1, segmentSize);
                if (segment.next.compareAndSet(null, candidate)) {
                    allocatedSegments.incrementAndGet();
                    next = candidate;
                } else {
                    next = segment.next.get();
                }
            }
            segment = next;
        }
        return segment;
    }

    private static final class Segment {
        private final long id;
        private final AtomicReferenceArray<TaskEnvelope> slots;
        private final AtomicReference<Segment> next = new AtomicReference<>();

        private Segment(long id, int segmentSize) {
            this.id = id;
            this.slots = new AtomicReferenceArray<>(segmentSize);
        }
    }

    private final class Reservation implements TaskReservation {
        private final long sequence;
        private final Segment segment;
        private final int offset;
        private final AtomicBoolean completed = new AtomicBoolean();

        private Reservation(long sequence, Segment segment, int offset) {
            this.sequence = sequence;
            this.segment = segment;
            this.offset = offset;
        }

        @Override
        public long sequence() {
            return sequence;
        }

        @Override
        public void publish(AcceptedTask<?> task) {
            if (task.acceptedSequence() != sequence) {
                throw new IllegalArgumentException("task.acceptedSequence 与 reservation.sequence 不一致");
            }
            complete(TaskEnvelope.task(task));
        }

        @Override
        public void abort() {
            complete(TaskEnvelope.TOMBSTONE);
        }

        private void complete(TaskEnvelope envelope) {
            if (!completed.compareAndSet(false, true)) {
                throw new IllegalStateException("reservation 已完成");
            }
            if (!segment.slots.compareAndSet(offset, null, envelope)) {
                throw new IllegalStateException("reservation 槽位已发布");
            }
        }
    }
}
