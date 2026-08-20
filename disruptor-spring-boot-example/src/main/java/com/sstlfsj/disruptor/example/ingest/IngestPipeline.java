package com.sstlfsj.disruptor.example.ingest;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** demo3：并行分片 4；ShardKeyed 保证同 key 落同一分片、按 seq 顺序处理。 */
@Component
public class IngestPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestPipeline.class);

    static volatile CountDownLatch latch;
    /** key -> 该 key 被观察到的（线程名 + seq）序列，供 runner 打印验证。 */
    static final Map<String, List<String>> observed = new ConcurrentHashMap<>();

    @DisruptorStage(pipeline = "ingest", name = "process", parallelism = 4)
    public void process(IngestEvent e) {
        String thread = Thread.currentThread().getName();
        observed.computeIfAbsent(e.getKey(), k -> new CopyOnWriteArrayList<>())
                .add(thread + "#seq" + e.getSeq());
        log.info("[ingest/process] key={} seq={} 线程={}", e.getKey(), e.getSeq(), thread);
        latch.countDown();
    }
}
