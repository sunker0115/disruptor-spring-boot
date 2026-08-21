package com.sstlfsj.disruptor.example.backpressure;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(5)
@RequiredArgsConstructor
public class BackpressureDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BackpressureDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo5 背压 tryPublish 三形态（backpressure）====");
        int dropped = 0;
        int total = 60;                         // 远超 buffer(16) + 慢消费 → 必然触发满
        for (int i = 0; i < total; i++) {
            int n = i;
            boolean ok = eventBus.tryPublish(BpEvent.class, e -> e.setN(n));
            if (!ok) {
                switch (i % 3) {                // 轮流演示三形态
                    case 0 -> {                 // ① 可丢弃：丢弃 + 计数
                        dropped++;
                    }
                    case 1 ->                    // ② 不能丢：降级落库补偿（此处用日志模拟）
                            log.info("[backpressure] n={} 降级落库补偿（事后重放）", n);
                    default -> {                 // ③ 关键链路：快速失败回推上游
                        try {
                            throw new IllegalStateException("系统繁忙，请稍后重试");
                        } catch (IllegalStateException ex) {
                            log.info("[backpressure] n={} 快速失败，回推上游限流：{}", n, ex.getMessage());
                        }
                    }
                }
            }
        }
        log.info("[backpressure] 本轮丢弃计数 dropped={}", dropped);
        Thread.sleep(1500);                     // 等已入队事件基本处理完，日志更完整
        results.markDone("backpressure");
        log.info("==== demo5 完成 ====");
    }
}
