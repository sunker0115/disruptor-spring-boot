package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.event.DisruptorStage;
import com.sstlfsj.disruptor.event.EventBus;
import com.sstlfsj.disruptor.event.Resettable;
import com.sstlfsj.disruptor.event.ShardKeyed;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 高级能力验收：ShardKeyed 同 key 保序、Resettable 事件叶子后重置复用。
 */
class PipelineAdvancedTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    // ---- ShardKeyed 同 key 保序 ----
    public static class KeyedEvent implements ShardKeyed {
        private String key;
        private int seq;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public int getSeq() {
            return seq;
        }

        public void setSeq(int seq) {
            this.seq = seq;
        }

        @Override
        public Object shardKey() {
            return key;
        }
    }

    static final List<Integer> KEY_A_ORDER = new CopyOnWriteArrayList<>();
    static final CountDownLatch KEYED_LATCH = new CountDownLatch(6);

    @Configuration
    static class KeyedConfig {
        @Bean
        KeyedStages keyedStages() {
            return new KeyedStages();
        }
    }

    static class KeyedStages {
        @DisruptorStage(pipeline = "keyed", name = "process", parallelism = 3)
        public void process(KeyedEvent e) {
            if ("A".equals(e.getKey())) {
                KEY_A_ORDER.add(e.getSeq());
            }
            KEYED_LATCH.countDown();
        }
    }

    @Test
    void sameKeyProcessedInPublishOrder() {
        runner.withUserConfiguration(KeyedConfig.class).run(ctx -> {
            EventBus bus = ctx.getBean(EventBus.class);
            // 交替发布 key A 与 key B；A 的 seq 递增，应由同一分片按发布顺序处理
            for (int i = 0; i < 3; i++) {
                int s = i;
                bus.publish(KeyedEvent.class, e -> {
                    e.setKey("A");
                    e.setSeq(s);
                });
                bus.publish(KeyedEvent.class, e -> {
                    e.setKey("B");
                    e.setSeq(s);
                });
            }
            assertTrue(KEYED_LATCH.await(3, TimeUnit.SECONDS), "全部应处理");
            assertEquals(List.of(0, 1, 2), KEY_A_ORDER, "同 key A 应由同一分片按发布顺序处理");
        });
    }

    // ---- Resettable 复用重置 ----
    public static class ResettableEvent implements Resettable {
        static final AtomicInteger RESET_COUNT = new AtomicInteger();
        static final CountDownLatch RESET_LATCH = new CountDownLatch(10);
        private int n;

        public void setN(int n) {
            this.n = n;
        }

        @Override
        public void reset() {
            RESET_COUNT.incrementAndGet();
            RESET_LATCH.countDown();
            this.n = 0;
        }
    }

    @Configuration
    static class ResettableConfig {
        @Bean
        ResettableStages resettableStages() {
            return new ResettableStages();
        }
    }

    static class ResettableStages {
        @DisruptorStage(pipeline = "reset", name = "process")
        public void process(ResettableEvent e) {
            // no-op
        }
    }

    @Test
    void resettableEventIsResetAfterLeaf() {
        runner.withPropertyValues("disruptor.buffer-size=8")
                .withUserConfiguration(ResettableConfig.class)
                .run(ctx -> {
                    EventBus bus = ctx.getBean(EventBus.class);
                    for (int i = 0; i < 10; i++) {
                        int v = i;
                        bus.publish(ResettableEvent.class, e -> e.setN(v));
                    }
                    assertTrue(ResettableEvent.RESET_LATCH.await(3, TimeUnit.SECONDS),
                            "每个事件在叶子后应被 reset");
                    assertEquals(10, ResettableEvent.RESET_COUNT.get(),
                            "reset 应对每个流经的事件各调用一次");
                });
    }
}
