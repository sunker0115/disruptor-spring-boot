package com.sstlfsj.disruptor.benchmark;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineSpec;
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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PublishPathBenchmark {

    private static final int BUFFER_SIZE = 65_536;
    private static final EventTranslator<BenchmarkEvent> TRANSLATOR =
            (event, sequence) -> event.value = sequence;

    @Benchmark
    public void nativeDisruptor(NativeState state) {
        state.ringBuffer.publishEvent(TRANSLATOR);
    }

    @Benchmark
    public void runtimeBuilt(RuntimeState state) {
        state.ringBuffer.publishEvent(TRANSLATOR);
    }

    @State(Scope.Benchmark)
    public static class NativeState {

        private Disruptor<BenchmarkEvent> disruptor;
        private RingBuffer<BenchmarkEvent> ringBuffer;

        @Setup
        public void setup() {
            disruptor = new Disruptor<>(BenchmarkEvent::new, BUFFER_SIZE, daemonThreadFactory("native"),
                    ProducerType.SINGLE, new BusySpinWaitStrategy());
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            });
            ringBuffer = disruptor.start();
        }

        @TearDown
        public void tearDown() {
            disruptor.shutdown();
        }
    }

    @State(Scope.Benchmark)
    public static class RuntimeState {

        private DisruptorRuntime runtime;
        private RingBuffer<BenchmarkEvent> ringBuffer;

        @Setup
        public void setup() {
            PipelineSpec<BenchmarkEvent> spec = PipelineSpec.builder(
                            "runtime", BenchmarkEvent.class, BenchmarkEvent::new)
                    .bufferSize(BUFFER_SIZE)
                    .producerType(ProducerType.SINGLE)
                    .waitStrategy(BusySpinWaitStrategy::new)
                    .threadFactory(daemonThreadFactory("runtime"))
                    .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                    }))
                    .build();
            runtime = DisruptorRuntime.builder().add(spec).build();
            ringBuffer = runtime.require("runtime", BenchmarkEvent.class).ringBuffer();
            runtime.start();
        }

        @TearDown
        public void tearDown() {
            runtime.shutdown();
        }
    }

    private static ThreadFactory daemonThreadFactory(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, "benchmark-" + name);
            thread.setDaemon(true);
            return thread;
        };
    }

    public static final class BenchmarkEvent {
        private long value;
    }
}
