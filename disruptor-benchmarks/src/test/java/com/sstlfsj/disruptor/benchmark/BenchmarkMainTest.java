package com.sstlfsj.disruptor.benchmark;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.runner.options.Options;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkMainTest {

    @Test
    void defaultsToEventLoopBenchmark() throws Exception {
        Options options = BenchmarkMain.options(new String[0]);

        assertThat(options.getIncludes()).hasSize(1);
        Pattern include = Pattern.compile(options.getIncludes().getFirst());
        assertThat(include.matcher(EventLoopBenchmark.class.getName() + ".boundedEventLoop").matches())
                .isTrue();
        assertThat(include.matcher("com.sstlfsj.disruptor.benchmark.CommonsEventLoopBenchmark.commonsBoundedEventLoop")
                .matches()).isFalse();
    }

    @Test
    void delegatesArgumentsToJmhCommandLineParser() throws Exception {
        Options options = BenchmarkMain.options(new String[]{
                "EventLoopBenchmark", "-wi", "1", "-i", "2", "-f", "1"
        });

        assertThat(options.getIncludes()).containsExactly("EventLoopBenchmark");
        assertThat(options.getWarmupIterations().get()).isEqualTo(1);
        assertThat(options.getMeasurementIterations().get()).isEqualTo(2);
        assertThat(options.getForkCount().get()).isEqualTo(1);
    }
}
