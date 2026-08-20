package com.sstlfsj.disruptor.example;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 各 demo 跑完后在此登记，供冒烟测试断言。 */
@Component
public class DemoResults {
    private final Map<String, Boolean> done = new ConcurrentHashMap<>();

    public void markDone(String demo) {
        done.put(demo, Boolean.TRUE);
    }

    public boolean isDone(String demo) {
        return done.getOrDefault(demo, Boolean.FALSE);
    }
}
