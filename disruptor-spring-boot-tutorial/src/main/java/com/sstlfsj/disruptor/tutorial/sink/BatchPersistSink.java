package com.sstlfsj.disruptor.tutorial.sink;

import com.sstlfsj.disruptor.tutorial.match.MatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量落库出口（演示 {@code endOfBatch}）：{@code persist} stage 逐条 {@link #stage} 成交，直到 Disruptor
 * 给出 {@code endOfBatch=true} 才 {@link #flush} 一次——把"当前可见的一批"合并成单次落库。
 *
 * <p>这正是 {@code endOfBatch} 的价值：闲时几乎每条都是批尾（批≈1，逐条低延迟落库），忙时消费者一次可见
 * 多条、批自动变大（一次落库摊薄 IO）。两批之间间隔不确定，故必须在批尾 flush，不能只靠攒够 N 条。
 * <b>生产环境把 {@link #flush} 换成一次 batch insert / 批量发 MQ。</b></p>
 *
 * <p>tutorial 用内存计数模拟落库。写侧唯一线程是 {@code persist} stage（parallelism=1），{@link #staging}
 * 用普通 ArrayList 无需加锁；统计字段被 web 线程读，故用 AtomicLong + volatile 保证可见性（单写多读）。</p>
 */
@Component
public class BatchPersistSink {

    private static final Logger log = LoggerFactory.getLogger(BatchPersistSink.class);

    private final List<MatchResult> staging = new ArrayList<>();

    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong persistedCount = new AtomicLong();
    private volatile long lastBatchSize = 0;
    private volatile long maxBatchSize = 0;

    /** 逐条攒入本批的成交（仅 {@code persist} 单线程调用），暂不落库。 */
    public void stage(List<MatchResult> results) {
        for (MatchResult r : results) {
            if (r instanceof MatchResult.Trade) {
                staging.add(r);
            }
        }
    }

    /** 批尾一次性落库当前 staging（仅 {@code persist} 单线程调用）；空批直接返回。 */
    public void flush() {
        if (staging.isEmpty()) {
            return;
        }
        int n = staging.size();
        // 模拟批量落库：真实场景这里是一次 batch insert / 批量发 MQ。
        staging.clear();
        persistedCount.addAndGet(n);
        flushCount.incrementAndGet();
        lastBatchSize = n;
        if (n > maxBatchSize) {
            maxBatchSize = n;
        }
        log.info("[matching/persist] 批量落库成交 {} 笔（累计 {} 笔 / {} 批）", n, persistedCount.get(), flushCount.get());
    }

    public long flushCount() { return flushCount.get(); }

    public long persistedCount() { return persistedCount.get(); }

    public long lastBatchSize() { return lastBatchSize; }

    public long maxBatchSize() { return maxBatchSize; }
}
