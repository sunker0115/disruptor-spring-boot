package com.sstlfsj.disruptor.benchmark;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class EventLoopBenchmarkSmokeTest {

    @ParameterizedTest
    @ValueSource(strings = {"bounded", "unbounded"})
    void discoversAndRunsSubmissionAndCompletedWorkloads(String backend) throws Exception {
        Options options = new OptionsBuilder()
                .include("^" + EventLoopBenchmark.class.getName() + ".*")
                .param("backend", backend)
                .warmupIterations(0)
                .measurementIterations(1)
                .measurementTime(TimeValue.milliseconds(50))
                .forks(0)
                .shouldFailOnError(true)
                .build();

        Collection<RunResult> results = new Runner(options).run();

        assertThat(results)
                .extracting(result -> result.getParams().getBenchmark())
                .containsExactlyInAnyOrder(
                        benchmark("nativeLmax"),
                        benchmark("coreManaged"),
                        benchmark("boundedEventLoop"),
                        benchmark("unboundedEventLoop"),
                        benchmark("jdkSingleThreadExecutor"),
                        benchmark("burstDrain"), benchmark("idleWakeup"),
                        benchmark("mpsc1"), benchmark("mpsc2"),
                        benchmark("mpsc4"), benchmark("mpsc8"));
        assertThat(results).allSatisfy(result -> {
            String name = result.getParams().getBenchmark();
            int producers = name.contains(".mpsc")
                    ? Integer.parseInt(name.substring(name.length() - 1)) : 1;
            assertThat(result.getParams().getThreads()).isEqualTo(producers);
            assertThat(result.getPrimaryResult().getScore()).isPositive();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"64:0", "64:8", "1024:8"})
    void discoversRealQueueLayersWithCorrectProducerCounts(String layout) throws Exception {
        String queueBenchmark = "com.sstlfsj.disruptor.concurrent.internal.UnboundedTaskQueueBenchmark";
        Collection<RunResult> results = new Runner(new OptionsBuilder()
                .include("^" + queueBenchmark + ".*")
                .param("layout", layout)
                .warmupIterations(0)
                .measurementIterations(1)
                .measurementTime(TimeValue.milliseconds(50))
                .forks(0)
                .shouldFailOnError(true)
                .build()).run();
        assertThat(results).extracting(result -> result.getParams().getBenchmark())
                .containsExactlyInAnyOrder(queueBenchmark + ".sameSegment",
                        queueBenchmark + ".mpsc1", queueBenchmark + ".mpsc2",
                        queueBenchmark + ".mpsc4", queueBenchmark + ".mpsc8");
        assertThat(results).allSatisfy(result -> {
            String name = result.getParams().getBenchmark();
            int producers = name.endsWith("sameSegment") ? 1
                    : Integer.parseInt(name.substring(name.length() - 1));
            assertThat(result.getParams().getThreads()).isEqualTo(producers);
            assertThat(result.getPrimaryResult().getScore()).isPositive();
        });
    }

    private static String benchmark(String method) {
        return EventLoopBenchmark.class.getName() + "." + method;
    }
}
