package com.sstlfsj.disruptor.benchmark;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.UnboundedEventLoop;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineHandle;
import com.sstlfsj.disruptor.core.PipelineSpec;
import com.sstlfsj.disruptor.core.PublicationResult;
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

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** 在相同单生产者、单消费者和计数工作负载下比较五条提交路径。 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class EventLoopBenchmark {

    private static final int QUEUE_SIZE = 65_536;
    private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(1);
    private static final EventTranslator<BenchmarkEvent> TRANSLATOR =
            (event, sequence) -> event.value = sequence;

    @Benchmark
    public void nativeLmax(NativeLmaxState state) {
        state.reserve();
        state.ringBuffer.publishEvent(TRANSLATOR);
    }

    @Benchmark
    public PublicationResult coreManaged(CoreManagedState state)
            throws InterruptedException {
        state.reserve();
        PublicationResult result = state.handle.publishEvent(TRANSLATOR, PUBLISH_TIMEOUT);
        if (result != PublicationResult.PUBLISHED) {
            throw new IllegalStateException("core managed 发布失败：" + result);
        }
        return result;
    }

    @Benchmark
    public void boundedEventLoop(BoundedEventLoopState state) {
        state.reserve();
        while (!state.eventLoop.tryExecute(state.command)) {
            Thread.onSpinWait();
        }
    }

    @Benchmark
    public void unboundedEventLoop(UnboundedEventLoopState state) {
        state.reserve();
        state.eventLoop.execute(state.command);
    }

    @Benchmark
    public void jdkSingleThreadExecutor(JdkExecutorState state) {
        state.reserve();
        state.executor.execute(state.command);
    }

    @State(Scope.Benchmark)
    public static class NativeLmaxState extends SubmissionState {

        private Disruptor<BenchmarkEvent> disruptor;
        private RingBuffer<BenchmarkEvent> ringBuffer;

        @Setup
        public void setup() {
            disruptor = new Disruptor<>(
                    BenchmarkEvent::new,
                    QUEUE_SIZE,
                    daemonThreadFactory("native-lmax"),
                    ProducerType.SINGLE,
                    new BusySpinWaitStrategy());
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> consumed++);
            ringBuffer = disruptor.start();
        }

        @TearDown
        public void tearDown() {
            disruptor.shutdown();
            verifyDrained();
        }
    }

    @State(Scope.Benchmark)
    public static class CoreManagedState extends SubmissionState {

        private DisruptorRuntime runtime;
        private PipelineHandle<BenchmarkEvent> handle;

        @Setup
        public void setup() {
            PipelineSpec<BenchmarkEvent> spec = PipelineSpec.builder(
                            "benchmark-core", BenchmarkEvent.class, BenchmarkEvent::new)
                    .bufferSize(QUEUE_SIZE)
                    .producerType(ProducerType.SINGLE)
                    .waitStrategy(BusySpinWaitStrategy::new)
                    .threadFactory(daemonThreadFactory("core-managed"))
                    .topology(disruptor -> disruptor.handleEventsWith(
                            (event, sequence, endOfBatch) -> consumed++))
                    .build();
            runtime = DisruptorRuntime.builder().add(spec).build();
            handle = runtime.require("benchmark-core", BenchmarkEvent.class);
            runtime.start();
        }

        @TearDown
        public void tearDown() {
            runtime.shutdown();
            verifyDrained();
        }
    }

    @State(Scope.Benchmark)
    public static class BoundedEventLoopState extends SubmissionState {

        private DisruptorEventLoop eventLoop;
        private final Runnable command = () -> consumed++;

        @Setup
        public void setup() {
            eventLoop = EventLoopBuilder.bounded("benchmark-bounded", QUEUE_SIZE)
                    .threadFactory(daemonThreadFactory("bounded-event-loop"))
                    .build();
            eventLoop.start().toCompletableFuture().join();
        }

        @TearDown
        public void tearDown() {
            eventLoop.shutdown();
            eventLoop.termination().toCompletableFuture().join();
            verifyDrained();
        }
    }

    @State(Scope.Benchmark)
    public static class UnboundedEventLoopState extends SubmissionState {

        private UnboundedEventLoop eventLoop;
        private final Runnable command = () -> consumed++;

        @Setup
        public void setup() {
            eventLoop = EventLoopBuilder.unbounded("benchmark-unbounded", 1024)
                    .threadFactory(daemonThreadFactory("unbounded-event-loop"))
                    .build();
            eventLoop.start().toCompletableFuture().join();
        }

        @TearDown
        public void tearDown() {
            eventLoop.shutdown();
            eventLoop.termination().toCompletableFuture().join();
            verifyDrained();
        }
    }

    @State(Scope.Benchmark)
    public static class JdkExecutorState extends SubmissionState {

        private ExecutorService executor;
        private final Runnable command = () -> consumed++;

        @Setup
        public void setup() {
            executor = Executors.newSingleThreadExecutor(
                    daemonThreadFactory("jdk-single-thread"));
        }

        @TearDown
        public void tearDown() throws InterruptedException {
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("JDK 单线程执行器未终止");
                }
            }
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
                        "基准关闭后仍有未消费任务：submitted=" + submitted + ", consumed=" + consumed);
            }
        }
    }

    private static ThreadFactory daemonThreadFactory(String name) {
        return command -> Thread.ofPlatform()
                .daemon(true)
                .name("benchmark-" + name)
                .unstarted(command);
    }

    public static final class BenchmarkEvent {
        private long value;
    }
}
