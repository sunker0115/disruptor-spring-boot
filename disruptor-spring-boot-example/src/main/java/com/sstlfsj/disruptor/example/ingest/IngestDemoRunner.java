package com.sstlfsj.disruptor.example.ingest;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
@Order(3)
@RequiredArgsConstructor
public class IngestDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo3 并行 + ShardKeyed 保序（ingest）====");
        List<String> keys = List.of("K1", "K2", "K3");
        int perKey = 4;
        IngestPipeline.observed.clear();
        IngestPipeline.latch = new CountDownLatch(keys.size() * perKey);
        for (int seq = 0; seq < perKey; seq++) {          // 交错发布：K1#0,K2#0,K3#0,K1#1,...
            for (String key : keys) {
                int s = seq;
                eventBus.publish(IngestEvent.class, e -> {
                    e.setKey(key);
                    e.setSeq(s);
                });
            }
        }
        if (!IngestPipeline.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo3 超时");
        }
        IngestPipeline.observed.forEach((key, seqs) ->
                log.info("[ingest] key={} 处理序列={}（同 key 应同一线程且 seq 递增）", key, seqs));
        results.markDone("ingest");
        log.info("==== demo3 完成 ====");
    }
}
