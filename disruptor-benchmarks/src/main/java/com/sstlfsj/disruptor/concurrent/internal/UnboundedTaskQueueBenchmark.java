package com.sstlfsj.disruptor.concurrent.internal;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * 直接访问真实 package-private 队列，不包含 admission、kernel 指标与 park/unpark。
 * sameSegment 每轮只测一次 256 槽的同段操作，setup 在计时外创建新队列；
 * 其 GC profiler 仍会统计 iteration setup 分配，不能用该 B/op 判断 ordinary 热路径分配。
 * mpsc 每个 JMH producer 提交一批后等待 release 完成，最多积压 threads * BATCH。
 * layout 是 segmentSize:maxPooledSegments 的配对参数：64:0/64:8 隔离复用收益，
 * 1024:8 观察较少换段的路径，避免两个独立参数的笛卡尔积。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class UnboundedTaskQueueBenchmark {

    private static final int BATCH = 256;
    private static final Runnable COMMAND = () -> { };

    @Benchmark
    @Threads(1)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 100, batchSize = 1)
    @Measurement(iterations = 20, batchSize = 1)
    @OperationsPerInvocation(BATCH)
    public long sameSegment(SameSegmentState state) {
        for (int index = 0; index < BATCH; index++) {
            publish(state.queue);
            if (!state.queue.poll()) {
                throw new IllegalStateException("同段普通任务未发布");
            }
            consume(state.queue);
        }
        return state.queue.consumerCursor();
    }

    @Benchmark
    @Threads(1)
    @OperationsPerInvocation(BATCH)
    public long mpsc1(QueueState state) {
        return publishAndDrain(state);
    }

    @Benchmark
    @Threads(2)
    @OperationsPerInvocation(BATCH)
    public long mpsc2(QueueState state) {
        return publishAndDrain(state);
    }

    @Benchmark
    @Threads(4)
    @OperationsPerInvocation(BATCH)
    public long mpsc4(QueueState state) {
        return publishAndDrain(state);
    }

    @Benchmark
    @Threads(8)
    @OperationsPerInvocation(BATCH)
    public long mpsc8(QueueState state) {
        return publishAndDrain(state);
    }

    private static long publishAndDrain(QueueState state) {
        long last = -1;
        for (int index = 0; index < BATCH; index++) {
            last = publish(state.queue);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (state.releasedSequence < last) {
            if (state.failure != null) {
                throw new IllegalStateException("队列消费者失败", state.failure);
            }
            if (System.nanoTime() - deadline >= 0) {
                throw new IllegalStateException("队列消费超时，不能把未完成提交计入吞吐");
            }
            Thread.onSpinWait();
        }
        return last;
    }

    private static long publish(TaskQueue queue) {
        long sequence = queue.tryClaim();
        if (sequence < 0) {
            throw new IllegalStateException("无界队列 claim 被拒绝");
        }
        queue.writeOrdinary(sequence, COMMAND);
        queue.publish(sequence);
        return sequence;
    }

    private static void consume(TaskQueue queue) {
        if (!queue.tryStartCurrentOrdinary()) {
            throw new IllegalStateException("已发布 ordinary 未能取得消费所有权");
        }
        Runnable command = queue.currentOrdinary();
        queue.advanceConsumer();
        command.run();
        queue.terminalizeCurrentOrdinary();
        queue.releaseCurrentSlot();
    }

    @State(Scope.Thread)
    public static class SameSegmentState {

        private UnboundedTaskQueue queue;

        @Setup(Level.Iteration)
        public void setup() {
            queue = new UnboundedTaskQueue(1024, 0);
        }

        @TearDown(Level.Iteration)
        public void verify() {
            if (queue.claimedCursor() != BATCH - 1 || queue.pending() != 0
                    || queue.segmentSnapshot().allocated() != 1
                    || queue.retainedReferences() != 0) {
                throw new IllegalStateException("同段基准必须每轮单次执行并完全释放 256 个槽");
            }
        }
    }

    @State(Scope.Benchmark)
    public static class QueueState {

        @Param({"64:0", "64:8", "1024:8"})
        public String layout;

        private UnboundedTaskQueue queue;
        private Thread consumer;
        // 唯一消费者写入；生产者只读。必须在 releaseCurrentSlot 之后发布完成进度。
        private volatile long releasedSequence;
        private volatile boolean stopping;
        private volatile Throwable failure;

        @Setup
        public void setup() {
            String[] parts = layout.split(":");
            queue = new UnboundedTaskQueue(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
            releasedSequence = -1;
            stopping = false;
            failure = null;
            consumer = Thread.ofPlatform().daemon(true).name("queue-benchmark-consumer")
                    .start(() -> {
                        try {
                            while (!stopping) {
                                if (!queue.poll()) {
                                    Thread.onSpinWait();
                                    continue;
                                }
                                consume(queue);
                                releasedSequence = queue.consumerCursor();
                            }
                        } catch (Throwable problem) {
                            failure = problem;
                        }
                    });
        }

        @TearDown
        public void tearDown() throws InterruptedException {
            stopping = true;
            consumer.join(10_000);
            if (consumer.isAlive() || failure != null) {
                throw new IllegalStateException("队列消费者未正常停止", failure);
            }
            if (releasedSequence != queue.claimedCursor() || queue.pending() != 0
                    || queue.retainedReferences() != 0) {
                throw new IllegalStateException("队列基准仍有未完成任务");
            }
        }
    }
}
