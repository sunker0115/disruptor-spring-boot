package com.sstlfsj.disruptor.example.ingest;

import com.lmax.disruptor.EventHandler;
import com.sstlfsj.disruptor.core.PipelineSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** demo3：使用 LMAX FAQ 推荐的取模 handler 模式，保证同 key 落同一分片。 */
@Component
public class IngestPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestPipeline.class);

    static volatile CountDownLatch latch;
    /** key -> 该 key 被观察到的（线程名 + seq）序列，供 runner 打印验证。 */
    static final Map<String, List<String>> observed = new ConcurrentHashMap<>();

    @Bean
    public PipelineSpec<IngestEvent> ingestPipelineSpec() {
        return PipelineSpec.builder("ingest", IngestEvent.class, IngestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(stripedHandlers(4)))
                .build();
    }

    @SuppressWarnings("unchecked")
    private EventHandler<IngestEvent>[] stripedHandlers(int stripes) {
        EventHandler<IngestEvent>[] handlers = new EventHandler[stripes];
        for (int stripe = 0; stripe < stripes; stripe++) {
            int stripeId = stripe;
            handlers[stripe] = (event, sequence, endOfBatch) -> {
                if (Math.floorMod(event.getKey().hashCode(), stripes) == stripeId) {
                    process(event);
                }
            };
        }
        return handlers;
    }

    public void process(IngestEvent e) {
        String thread = Thread.currentThread().getName();
        observed.computeIfAbsent(e.getKey(), k -> new CopyOnWriteArrayList<>())
                .add(thread + "#seq" + e.getSeq());
        log.info("[ingest/process] key={} seq={} 线程={}", e.getKey(), e.getSeq(), thread);
        latch.countDown();
    }
}
