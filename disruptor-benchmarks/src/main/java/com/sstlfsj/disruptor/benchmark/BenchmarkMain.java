package com.sstlfsj.disruptor.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.regex.Pattern;

public final class BenchmarkMain {

    private BenchmarkMain() {
    }

    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
        new Runner(options(args)).run();
    }

    static Options options(String[] args) throws CommandLineOptionException {
        if (args.length > 0) {
            return new CommandLineOptions(args);
        }
        return new OptionsBuilder()
                .include("^" + Pattern.quote(EventLoopBenchmark.class.getName()) + "\\..*$")
                .build();
    }
}
