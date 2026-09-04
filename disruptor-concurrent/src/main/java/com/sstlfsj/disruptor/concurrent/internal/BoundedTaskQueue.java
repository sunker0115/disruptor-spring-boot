package com.sstlfsj.disruptor.concurrent.internal;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** 使用 LMAX multi-producer RingBuffer 的有界 TaskQueue。 */
final class BoundedTaskQueue implements TaskQueue {

    private final RingBuffer<Cell> ringBuffer;
    private final Sequence consumerSequence = new Sequence(RingBuffer.INITIAL_CURSOR_VALUE);
    private final AtomicLong pending = new AtomicLong();

    private long nextConsumerSequence;

    BoundedTaskQueue(int capacity) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity 必须为 2 的幂，实际值=" + capacity);
        }
        ringBuffer = RingBuffer.createMultiProducer(Cell::new, capacity);
        ringBuffer.addGatingSequences(consumerSequence);
    }

    @Override
    public TaskReservation tryReserve() {
        try {
            long sequence = ringBuffer.tryNext();
            pending.incrementAndGet();
            return new Reservation(sequence);
        } catch (InsufficientCapacityException ignored) {
            return null;
        }
    }

    @Override
    public AcceptedTask<?> poll() {
        while (ringBuffer.isAvailable(nextConsumerSequence)) {
            long sequence = nextConsumerSequence++;
            Cell cell = ringBuffer.get(sequence);
            TaskEnvelope envelope = cell.envelope;
            if (envelope == null) {
                throw new IllegalStateException("已发布的 RingBuffer 槽位不能为空");
            }
            cell.envelope = null;
            consumerSequence.set(sequence);
            pending.decrementAndGet();
            if (!envelope.tombstone()) {
                return envelope.task();
            }
        }
        return null;
    }

    @Override
    public long pending() {
        return pending.get();
    }

    @Override
    public long remainingCapacity() {
        return ringBuffer.remainingCapacity();
    }

    @Override
    public int allocatedSegments() {
        return 0;
    }

    int retainedReferences() {
        int retained = 0;
        for (int index = 0; index < ringBuffer.getBufferSize(); index++) {
            if (ringBuffer.get(index).envelope != null) {
                retained++;
            }
        }
        return retained;
    }

    private void publish(long sequence, TaskEnvelope envelope) {
        ringBuffer.get(sequence).envelope = envelope;
        ringBuffer.publish(sequence);
    }

    private static final class Cell {
        private TaskEnvelope envelope;
    }

    private final class Reservation implements TaskReservation {
        private final long sequence;
        private final AtomicBoolean completed = new AtomicBoolean();

        private Reservation(long sequence) {
            this.sequence = sequence;
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
            BoundedTaskQueue.this.publish(sequence, envelope);
        }
    }
}
