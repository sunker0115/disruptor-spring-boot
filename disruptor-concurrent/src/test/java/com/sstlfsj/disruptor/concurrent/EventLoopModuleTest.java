package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventLoopModuleTest {

    @Test
    void startsInDeclarationOrderAndStopsInReverseOrder() throws Exception {
        List<String> events = new ArrayList<>();
        DisruptorEventLoop loop = EventLoopBuilder.bounded("ordered-modules", 8)
                .module(module("first", events))
                .module(module("second", events))
                .shutdownTimeout(Duration.ofSeconds(2))
                .build();

        loop.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        loop.shutdownNow();
        loop.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertEquals(List.of("start-first", "start-second", "stop-second", "stop-first"),
                events);
    }

    private static EventLoopModule module(String name, List<String> events) {
        return new EventLoopModule() {
            @Override
            public void onStart(EventLoop loop) {
                events.add("start-" + name);
            }

            @Override
            public void onStop(EventLoop loop) {
                events.add("stop-" + name);
            }
        };
    }
}
