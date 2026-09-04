package com.sstlfsj.disruptor.concurrent.internal;

import com.sstlfsj.disruptor.concurrent.EventLoopModule;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ModuleLifecycleTest {

    @Test
    void startsInDeclarationOrderAndStopsSuccessfulPrefixInReverseOnce() throws Exception {
        List<String> actions = new ArrayList<>();
        EventLoopModule first = module("first", actions);
        EventLoopModule second = module("second", actions);
        ModuleLifecycle lifecycle = new ModuleLifecycle(List.of(first, second));

        lifecycle.start(null);
        Throwable failure = lifecycle.stop(null, null);
        lifecycle.stop(null, null);

        assertEquals(List.of("start:first", "start:second", "stop:second", "stop:first"), actions);
        assertEquals(null, failure);
    }

    @Test
    void startFailurePreservesPrimaryAndSuppressesEveryStopFailure() {
        List<String> actions = new ArrayList<>();
        IllegalStateException startFailure = new IllegalStateException("start");
        IllegalStateException secondStopFailure = new IllegalStateException("stop-second");
        IllegalStateException firstStopFailure = new IllegalStateException("stop-first");
        EventLoopModule first = failingStopModule("first", actions, firstStopFailure);
        EventLoopModule second = failingStopModule("second", actions, secondStopFailure);
        EventLoopModule third = new EventLoopModule() {
            @Override
            public void onStart(com.sstlfsj.disruptor.concurrent.EventLoop loop) {
                actions.add("start:third");
                throw startFailure;
            }
        };
        ModuleLifecycle lifecycle = new ModuleLifecycle(List.of(first, second, third));

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> lifecycle.start(null));
        assertSame(startFailure, thrown);
        Throwable result = lifecycle.stop(null, thrown);

        assertSame(startFailure, result);
        assertEquals(List.of(secondStopFailure, firstStopFailure), List.of(result.getSuppressed()));
        assertEquals(List.of("start:first", "start:second", "start:third", "stop:second", "stop:first"),
                actions);
    }

    @Test
    void updateFailureRemainsPrimaryWhenStopAlsoFails() throws Exception {
        IllegalStateException updateFailure = new IllegalStateException("update");
        IllegalStateException stopFailure = new IllegalStateException("stop");
        EventLoopModule module = new EventLoopModule() {
            @Override
            public void onUpdate(com.sstlfsj.disruptor.concurrent.EventLoop loop, long nowNanos) {
                throw updateFailure;
            }

            @Override
            public void onStop(com.sstlfsj.disruptor.concurrent.EventLoop loop) {
                throw stopFailure;
            }
        };
        ModuleLifecycle lifecycle = new ModuleLifecycle(List.of(module));
        lifecycle.start(null);

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> lifecycle.update(null, 1));
        Throwable result = lifecycle.stop(null, thrown);

        assertSame(updateFailure, result);
        assertEquals(List.of(stopFailure), List.of(result.getSuppressed()));
    }

    private static EventLoopModule module(String name, List<String> actions) {
        return new EventLoopModule() {
            @Override
            public void onStart(com.sstlfsj.disruptor.concurrent.EventLoop loop) {
                actions.add("start:" + name);
            }

            @Override
            public void onStop(com.sstlfsj.disruptor.concurrent.EventLoop loop) {
                actions.add("stop:" + name);
            }
        };
    }

    private static EventLoopModule failingStopModule(
            String name,
            List<String> actions,
            RuntimeException failure) {
        return new EventLoopModule() {
            @Override
            public void onStart(com.sstlfsj.disruptor.concurrent.EventLoop loop) {
                actions.add("start:" + name);
            }

            @Override
            public void onStop(com.sstlfsj.disruptor.concurrent.EventLoop loop) {
                actions.add("stop:" + name);
                throw failure;
            }
        };
    }
}
