package com.sstlfsj.disruptor.benchmark;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class EventLoopBenchmarkSmokeTest {

    @Test
    void discoversAndRunsAllFiveSubmissionPaths() throws Exception {
        Options options = new OptionsBuilder()
                .include("^" + EventLoopBenchmark.class.getName() + ".*")
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
                        benchmark("jdkSingleThreadExecutor"));
        assertThat(results)
                .allSatisfy(result -> assertThat(result.getPrimaryResult().getScore()).isPositive());
    }

    private static String benchmark(String method) {
        return EventLoopBenchmark.class.getName() + "." + method;
    }
}
