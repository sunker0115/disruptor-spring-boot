package com.sstlfsj.disruptor.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public final class BenchmarkMain {

    private BenchmarkMain() {
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(PublishPathBenchmark.class.getSimpleName())
                .build();
        new Runner(options).run();
    }
}
