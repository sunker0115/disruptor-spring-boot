package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.event.DisruptorStage;
import com.sstlfsj.disruptor.event.EventBus;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 强类型管道端到端验收：填充式发布、DAG 线性/菱形顺序、并行分片、背压、零分配复用。
 */
class TypedPipelineTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    public static class OrderEvent {
        private String id;
        private long amount;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public long getAmount() {
            return amount;
        }

        public void setAmount(long amount) {
            this.amount = amount;
        }
    }

    // ---- 线性 validate -> persist ----
    static final List<String> LINEAR_TRACE = new CopyOnWriteArrayList<>();
    static final CountDownLatch LINEAR_LATCH = new CountDownLatch(2);

    @Configuration
    static class LinearConfig {
        @Bean
        LinearStages linearStages() {
            return new LinearStages();
        }
    }

    static class LinearStages {
        @DisruptorStage(pipeline = "order", name = "validate")
        public void validate(OrderEvent e) {
            LINEAR_TRACE.add("validate:" + e.getId() + ":" + e.getAmount());
            LINEAR_LATCH.countDown();
        }

        @DisruptorStage(pipeline = "order", name = "persist", after = "validate")
        public void persist(OrderEvent e) {
            LINEAR_TRACE.add("persist:" + e.getId());
            LINEAR_LATCH.countDown();
        }
    }

    @Test
    void fillPublishFlowsThroughStagesInOrder() {
        runner.withUserConfiguration(LinearConfig.class).run(ctx -> {
            EventBus bus = ctx.getBean(EventBus.class);
            bus.publish(OrderEvent.class, e -> {
                e.setId("A");
                e.setAmount(100);
            });
            assertTrue(LINEAR_LATCH.await(3, TimeUnit.SECONDS), "两阶段都应处理");
            assertEquals(List.of("validate:A:100", "persist:A"), LINEAR_TRACE,
                    "事件应按 validate -> persist 顺序流经，且填充值可见");
        });
    }

    // ---- 菱形 validate -> (persist, audit) -> notify ----
    public static class PayEvent {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    static final List<String> DIAMOND_TRACE = new CopyOnWriteArrayList<>();
    static final CountDownLatch DIAMOND_LATCH = new CountDownLatch(4);

    @Configuration
    static class DiamondConfig {
        @Bean
        DiamondStages diamondStages() {
            return new DiamondStages();
        }
    }

    static class DiamondStages {
        @DisruptorStage(pipeline = "pay", name = "validate")
        public void validate(PayEvent e) {
            DIAMOND_TRACE.add("validate");
            DIAMOND_LATCH.countDown();
        }

        @DisruptorStage(pipeline = "pay", name = "persist", after = "validate")
        public void persist(PayEvent e) {
            DIAMOND_TRACE.add("persist");
            DIAMOND_LATCH.countDown();
        }

        @DisruptorStage(pipeline = "pay", name = "audit", after = "validate")
        public void audit(PayEvent e) {
            DIAMOND_TRACE.add("audit");
            DIAMOND_LATCH.countDown();
        }

        @DisruptorStage(pipeline = "pay", name = "notify", after = {"persist", "audit"})
        public void notify(PayEvent e) {
            DIAMOND_TRACE.add("notify");
            DIAMOND_LATCH.countDown();
        }
    }

    @Test
    void diamondJoinRunsAfterAllBranches() {
        runner.withUserConfiguration(DiamondConfig.class).run(ctx -> {
            ctx.getBean(EventBus.class).publish(PayEvent.class, e -> e.setId("P"));
            assertTrue(DIAMOND_LATCH.await(3, TimeUnit.SECONDS), "四阶段都应处理");
            assertEquals("validate", DIAMOND_TRACE.get(0), "validate 最先");
            assertEquals("notify", DIAMOND_TRACE.get(3), "notify 最后（汇聚 persist、audit 之后）");
        });
    }

    // ---- 并行分片：每事件恰由一个分片处理 ----
    public static class WorkEvent {
        private int n;

        public int getN() {
            return n;
        }

        public void setN(int n) {
            this.n = n;
        }
    }

    static final AtomicInteger WORK_COUNT = new AtomicInteger();
    static final CountDownLatch WORK_LATCH = new CountDownLatch(40);

    @Configuration
    static class ParallelConfig {
        @Bean
        ParallelStages parallelStages() {
            return new ParallelStages();
        }
    }

    static class ParallelStages {
        @DisruptorStage(pipeline = "work", name = "process", parallelism = 4)
        public void process(WorkEvent e) {
            WORK_COUNT.incrementAndGet();
            WORK_LATCH.countDown();
        }
    }

    @Test
    void parallelStageProcessesEachEventExactlyOnce() {
        runner.withUserConfiguration(ParallelConfig.class).run(ctx -> {
            EventBus bus = ctx.getBean(EventBus.class);
            for (int i = 0; i < 40; i++) {
                int v = i;
                bus.publish(WorkEvent.class, e -> e.setN(v));
            }
            assertTrue(WORK_LATCH.await(3, TimeUnit.SECONDS), "全部事件应被处理");
            assertEquals(40, WORK_COUNT.get(), "4 分片下每个事件恰好处理一次（无重复无遗漏）");
        });
    }

    // ---- 背压 ----
    public static class BpEvent {
        private int n;

        public void setN(int n) {
            this.n = n;
        }
    }

    @Configuration
    static class BackpressureConfig {
        @Bean
        BackpressureStages backpressureStages() {
            return new BackpressureStages();
        }
    }

    static final CountDownLatch BP_BLOCK = new CountDownLatch(1);
    static final CountDownLatch BP_CONSUMING = new CountDownLatch(1);

    static class BackpressureStages {
        @DisruptorStage(pipeline = "bp", name = "block")
        public void block(BpEvent e) {
            BP_CONSUMING.countDown();
            try {
                BP_BLOCK.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void tryPublishReturnsFalseWhenFull() {
        runner.withPropertyValues("disruptor.buffer-size=8")
                .withUserConfiguration(BackpressureConfig.class)
                .run(ctx -> {
                    EventBus bus = ctx.getBean(EventBus.class);
                    bus.publish(BpEvent.class, e -> e.setN(0)); // 让消费者卡住
                    assertTrue(BP_CONSUMING.await(3, TimeUnit.SECONDS), "消费者应已取走首个事件并阻塞");
                    boolean sawFalse = false;
                    for (int i = 0; i < 100; i++) {
                        if (!bus.tryPublish(BpEvent.class, e -> e.setN(1))) {
                            sawFalse = true;
                            break;
                        }
                    }
                    BP_BLOCK.countDown();
                    assertTrue(sawFalse, "buffer 满时 tryPublish 应返回 false");
                    assertTrue(bus.remainingCapacity(BpEvent.class) >= 0, "remainingCapacity 可查询");
                });
    }

    // ---- 零分配：事件对象被复用（预分配，非每次 new） ----
    public static class ReuseEvent {
        private int n;

        public void setN(int n) {
            this.n = n;
        }
    }

    static final Set<Integer> IDENTITIES = ConcurrentHashMap.newKeySet();
    static final CountDownLatch REUSE_LATCH = new CountDownLatch(32);

    @Configuration
    static class ReuseConfig {
        @Bean
        ReuseStages reuseStages() {
            return new ReuseStages();
        }
    }

    static class ReuseStages {
        @DisruptorStage(pipeline = "reuse", name = "collect")
        public void collect(ReuseEvent e) {
            IDENTITIES.add(System.identityHashCode(e));
            REUSE_LATCH.countDown();
        }
    }

    @Test
    void eventsArePreallocatedAndReused() {
        runner.withPropertyValues("disruptor.buffer-size=8")
                .withUserConfiguration(ReuseConfig.class)
                .run(ctx -> {
                    EventBus bus = ctx.getBean(EventBus.class);
                    for (int i = 0; i < 32; i++) {
                        int v = i;
                        bus.publish(ReuseEvent.class, e -> e.setN(v));
                    }
                    assertTrue(REUSE_LATCH.await(3, TimeUnit.SECONDS), "全部应处理");
                    assertTrue(IDENTITIES.size() <= 8,
                            "事件对象应从预分配的 bufferSize(=8) 个中复用，而非每次 new（实际不同实例数="
                                    + IDENTITIES.size() + "）");
                });
    }
}
