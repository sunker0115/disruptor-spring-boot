package com.sstlfsj.disruptor.tutorial;

import com.sstlfsj.disruptor.tutorial.match.MatchEngine;
import com.sstlfsj.disruptor.tutorial.match.MatchResult;
import com.sstlfsj.disruptor.tutorial.match.Order;
import com.sstlfsj.disruptor.tutorial.match.Side;
import com.sstlfsj.disruptor.tutorial.dto.PlaceOrderRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 背压：撮合消费者被闸门卡住 → 小 RingBuffer 灌满 → {@code tryPublish} 返回 false → HTTP 429。
 * 用 {@code @Primary} 覆盖 MatchEngine 为阻塞子类，确定性触发（对照 example/backpressure 的"灌满即拒"）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "disruptor.buffer-size=8",
        "disruptor.shutdown-timeout=2s"
})
@Import(BackpressureTest.GatedConfig.class)
class BackpressureTest {

    @LocalServerPort
    private int port;

    @Autowired
    private GatedMatchEngine engine;

    private RestClient client;

    @BeforeEach
    void setUp() {
        client = RestClient.create("http://localhost:" + port);
    }

    @AfterEach
    void release() {
        engine.release();   // 放行，避免关停时排空阻塞
    }

    @Test
    void ringBufferFullReturns429() {
        int total = 50;
        int accepted = 0;
        int rejected = 0;
        for (int i = 0; i < total; i++) {
            HttpStatusCode status = client.post().uri("/orders")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new PlaceOrderRequest("BP", Side.BUY, new BigDecimal("100"), new BigDecimal("1")))
                    .exchange((req, res) -> res.getStatusCode());
            if (status == HttpStatus.ACCEPTED) {
                accepted++;
            } else if (status == HttpStatus.TOO_MANY_REQUESTS) {
                rejected++;
            }
        }
        assertThat(accepted).isPositive();   // 缓冲未满前受理
        assertThat(rejected).isPositive();   // 灌满后触发背压 429

        int acceptedCount = accepted;
        engine.release();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(engine.handledCount()).isEqualTo(acceptedCount));
    }

    @TestConfiguration
    static class GatedConfig {
        @Bean
        @Primary
        GatedMatchEngine gatedMatchEngine() {
            return new GatedMatchEngine();
        }
    }

    /** handle() 阻塞在闸门上，模拟撮合停摆使 RingBuffer 灌满。 */
    static class GatedMatchEngine extends MatchEngine {
        private final CountDownLatch gate = new CountDownLatch(1);
        private final AtomicInteger handled = new AtomicInteger();

        @Override
        public List<MatchResult> handle(Order order) {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            List<MatchResult> results = super.handle(order);
            handled.incrementAndGet();
            return results;
        }

        void release() {
            gate.countDown();
        }

        int handledCount() {
            return handled.get();
        }
    }
}
