package com.sstlfsj.disruptor.tutorial;

import com.sstlfsj.disruptor.tutorial.match.Side;
import com.sstlfsj.disruptor.tutorial.dto.AcceptedResponse;
import com.sstlfsj.disruptor.tutorial.dto.BookResponse;
import com.sstlfsj.disruptor.tutorial.dto.PersistStatsResponse;
import com.sstlfsj.disruptor.tutorial.dto.PlaceOrderRequest;
import com.sstlfsj.disruptor.tutorial.dto.StatsResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 端到端：真实 HTTP 下单 → 后台异步撮合 → 读侧观测。撮合异步，故用 Awaitility 轮询到期望再断言
 * （不 Thread.sleep）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MatchingFlowTest {

    @LocalServerPort
    private int port;

    private RestClient client;

    @BeforeEach
    void setUp() {
        client = RestClient.create("http://localhost:" + port);
    }

    private ResponseEntity<AcceptedResponse> place(String symbol, Side side, String price, String qty) {
        return client.post().uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new PlaceOrderRequest(symbol, side, new BigDecimal(price), new BigDecimal(qty)))
                .retrieve()
                .toEntity(AcceptedResponse.class);
    }

    @Test
    void crossingOrdersProduceTradeVisibleInStats() {
        String sym = "FLOWA";
        ResponseEntity<AcceptedResponse> sell = place(sym, Side.SELL, "100", "10");
        assertThat(sell.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(sell.getBody()).isNotNull();
        assertThat(sell.getBody().orderId()).isPositive();

        assertThat(place(sym, Side.BUY, "100", "10").getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            StatsResponse stats = client.get().uri("/orders/stats").retrieve().body(StatsResponse.class);
            assertThat(stats).isNotNull();
            assertThat(stats.tradeCount()).isEqualTo(1);
            assertThat(stats.tradedVolume()).isEqualByComparingTo("10");

            // persist stage 与 metrics stage 并行消费同一撮合产出：落库成交数应与统计成交数恒等，
            // 且落库发生在 endOfBatch。批大小依赖调度时序，故只断言「两条 stage 结果恒等」+「flush 已发生」，
            // 不断言具体批大小（自适应批处理的批变大效果留给日志/手动压测观察）。
            PersistStatsResponse persist =
                    client.get().uri("/orders/persist-stats").retrieve().body(PersistStatsResponse.class);
            assertThat(persist).isNotNull();
            assertThat(persist.persistedCount()).isEqualTo(stats.tradeCount());
            assertThat(persist.flushCount()).isGreaterThanOrEqualTo(1);
            assertThat(persist.lastBatchSize()).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void nonCrossingOrderRestsInBook() {
        String sym = "FLOWB";
        assertThat(place(sym, Side.BUY, "90", "5").getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            BookResponse book = client.get().uri("/book?symbol=" + sym).retrieve().body(BookResponse.class);
            assertThat(book).isNotNull();
            assertThat(book.bids()).singleElement().satisfies(l -> {
                assertThat(l.price()).isEqualByComparingTo("90");
                assertThat(l.quantity()).isEqualByComparingTo("5");
            });
            assertThat(book.asks()).isEmpty();
        });
    }
}
