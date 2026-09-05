package com.sstlfsj.disruptor.benchmark;

import cn.wjybxx.concurrent.AgentEvent;
import cn.wjybxx.concurrent.DefaultThreadFactory;
import cn.wjybxx.concurrent.EventLoopBuilder;
import cn.wjybxx.concurrent.IEventLoop;
import cn.wjybxx.disruptor.MpUnboundedEventSequencer;
import cn.wjybxx.disruptor.RingBufferEventSequencer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/** 在相同单生产者、单消费者和计数工作负载下测量 Commons EventLoop。 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class CommonsEventLoopBenchmark {

    private static final int QUEUE_SIZE = 65_536;

    @Benchmark
    public void commonsBoundedEventLoop(BoundedState state) {
        state.reserve();
        state.eventLoop.execute(state.command);
    }

    @Benchmark
    public void commonsUnboundedEventLoop(UnboundedState state) {
        state.reserve();
        state.eventLoop.execute(state.command);
    }

    @State(Scope.Benchmark)
    public static class BoundedState extends SubmissionState {

        private IEventLoop eventLoop;
        private final Runnable command = () -> consumed++;

        @Setup
        public void setup() {
            eventLoop = EventLoopBuilder.newDisruptBuilder(
                            RingBufferEventSequencer.newMultiProducer(AgentEvent::new)
                                    .setBufferSize(QUEUE_SIZE)
                                    .build())
                    .setThreadFactory(new DefaultThreadFactory("benchmark-commons-bounded", true))
                    .build();
            eventLoop.start().join();
        }

        @TearDown
        public void tearDown() {
            eventLoop.shutdown();
            eventLoop.terminationFuture().join();
            verifyDrained();
        }
    }

    @State(Scope.Benchmark)
    public static class UnboundedState extends SubmissionState {

        private IEventLoop eventLoop;
        private final Runnable command = () -> consumed++;

        @Setup
        public void setup() {
            eventLoop = EventLoopBuilder.newDisruptBuilder(
                            MpUnboundedEventSequencer.newBuilder(AgentEvent::new)
                                    .setChunkSize(1024)
                                    .build())
                    .setThreadFactory(new DefaultThreadFactory("benchmark-commons-unbounded", true))
                    .build();
            eventLoop.start().join();
        }

        @TearDown
        public void tearDown() {
            eventLoop.shutdown();
            eventLoop.terminationFuture().join();
            verifyDrained();
        }
    }

    abstract static class SubmissionState {

        protected volatile long consumed;
        private long submitted;

        final void reserve() {
            while (submitted - consumed >= QUEUE_SIZE) {
                Thread.onSpinWait();
            }
            submitted++;
        }

        final void verifyDrained() {
            if (submitted != consumed) {
                throw new IllegalStateException(
                        "Commons 基准关闭后仍有未消费任务：submitted=" + submitted + ", consumed=" + consumed);
            }
        }
    }
}
