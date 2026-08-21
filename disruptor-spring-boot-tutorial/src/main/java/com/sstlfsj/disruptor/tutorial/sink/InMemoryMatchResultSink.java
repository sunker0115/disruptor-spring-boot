package com.sstlfsj.disruptor.tutorial.sink;

import com.sstlfsj.disruptor.tutorial.match.MatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * 内存出口（占位）：仅收集最近若干条撮合结果供调试观察，不落库/不发 MQ。
 * {@code emit} stage 单线程写；{@link #recent(int)} 可能被其它线程读，故用 {@code synchronized} 护住。
 */
@Component
public class InMemoryMatchResultSink implements MatchResultSink {

    private static final Logger log = LoggerFactory.getLogger(InMemoryMatchResultSink.class);
    private static final int MAX_RETAINED = 1000;

    private final Deque<MatchResult> recent = new ArrayDeque<>();

    @Override
    public void accept(List<MatchResult> results) {
        if (results.isEmpty()) {
            return;
        }
        synchronized (recent) {
            for (MatchResult r : results) {
                recent.addLast(r);
                if (recent.size() > MAX_RETAINED) {
                    recent.removeFirst();
                }
            }
        }
        log.info("[matching/emit] 下发 {} 条撮合结果", results.size());
    }

    /** 最近 n 条（调试用）。 */
    public List<MatchResult> recent(int n) {
        synchronized (recent) {
            List<MatchResult> all = new ArrayList<>(recent);
            int from = Math.max(0, all.size() - n);
            return new ArrayList<>(all.subList(from, all.size()));
        }
    }
}
