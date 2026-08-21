package com.sstlfsj.disruptor.tutorial.match;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 撮合核心纯单测（不启 Spring）：验证撮合正确性、部分成交、不交叉挂单、价格时间优先、盘口深度。
 * 这是对 vendor 撮合语义的可执行审查。
 */
class MatchEngineTest {

    private static final String SYM = "BTCUSDT";

    private final MatchEngine engine = new MatchEngine();

    private static Order order(long id, Side side, String price, String qty) {
        return Order.builder()
                .orderId(id).symbol(SYM).side(side)
                .price(new BigDecimal(price)).quantity(new BigDecimal(qty))
                .transactTime(id)
                .build();
    }

    private static List<MatchResult.Trade> trades(List<MatchResult> out) {
        return out.stream().filter(r -> r instanceof MatchResult.Trade).map(r -> (MatchResult.Trade) r).toList();
    }

    @Test
    void crossingOrderFullyFills() {
        engine.handle(order(1, Side.SELL, "100", "10"));           // 挂卖
        List<MatchResult> out = engine.handle(order(2, Side.BUY, "100", "10"));  // 打进买，全成

        List<MatchResult.Trade> trades = trades(out);
        assertThat(trades).hasSize(1);
        MatchResult.Trade t = trades.get(0);
        assertThat(t.price()).isEqualByComparingTo("100");
        assertThat(t.quantity()).isEqualByComparingTo("10");
        assertThat(t.takerOrderId()).isEqualTo(2);
        assertThat(t.makerOrderId()).isEqualTo(1);
        // taker 完成
        assertThat(out).anyMatch(r -> r instanceof MatchResult.Done d && d.orderId() == 2 && d.taker());
        // 两侧盘口清空
        MatchEngine.BookDepth depth = engine.depth(SYM);
        assertThat(depth.bids()).isEmpty();
        assertThat(depth.asks()).isEmpty();
    }

    @Test
    void partialFillPlacesRemainderOnBook() {
        engine.handle(order(1, Side.SELL, "100", "5"));            // 卖 5
        List<MatchResult> out = engine.handle(order(2, Side.BUY, "100", "10")); // 买 10：吃 5、挂 5

        assertThat(trades(out)).singleElement()
                .satisfies(t -> assertThat(t.quantity()).isEqualByComparingTo("5"));
        // 买单剩余 5 进盘口
        assertThat(out).anyMatch(r -> r instanceof MatchResult.Open o
                && o.orderId() == 2 && o.remaining().compareTo(new BigDecimal("5")) == 0);
        MatchEngine.BookDepth depth = engine.depth(SYM);
        assertThat(depth.bids()).singleElement().satisfies(l -> {
            assertThat(l.price()).isEqualByComparingTo("100");
            assertThat(l.quantity()).isEqualByComparingTo("5");
        });
        assertThat(depth.asks()).isEmpty();
    }

    @Test
    void nonCrossingOrderRestsOnBook() {
        List<MatchResult> out = engine.handle(order(1, Side.BUY, "90", "5"));   // 无对手，挂单

        assertThat(trades(out)).isEmpty();
        assertThat(out).singleElement().isInstanceOf(MatchResult.Open.class);
        MatchEngine.BookDepth depth = engine.depth(SYM);
        assertThat(depth.bids()).singleElement().satisfies(l -> {
            assertThat(l.price()).isEqualByComparingTo("90");
            assertThat(l.quantity()).isEqualByComparingTo("5");
        });
        assertThat(depth.asks()).isEmpty();
    }

    @Test
    void priceTimePriority() {
        engine.handle(order(1, Side.SELL, "100", "5"));   // 100 档，先到
        engine.handle(order(2, Side.SELL, "100", "5"));   // 100 档，后到
        engine.handle(order(3, Side.SELL, "101", "5"));   // 101 档，更差价
        List<MatchResult> out = engine.handle(order(4, Side.BUY, "101", "15")); // 吃满 15

        List<MatchResult.Trade> trades = trades(out);
        // 先撮 100 档（id1→id2），再撮 101 档（id3）：价格优先 + 同价时间优先
        assertThat(trades).extracting(MatchResult.Trade::makerOrderId).containsExactly(1L, 2L, 3L);
        assertThat(trades).extracting(t -> t.price().stripTrailingZeros().toPlainString())
                .containsExactly("100", "100", "101");
        assertThat(out).anyMatch(r -> r instanceof MatchResult.Done d && d.orderId() == 4 && d.taker());
        assertThat(engine.depth(SYM).asks()).isEmpty();   // 卖盘吃光
    }
}
